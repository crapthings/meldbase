//go:build linux

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

func captureQualificationEnvironment(facts destructiveVolumeFacts, revision, method, operatorPath string, operatorBytes uint64, operatorSHA string) (qualificationEnvironmentEvidence, error) {
	evidence := qualificationEnvironmentBase(facts, revision, method, operatorPath, operatorBytes, operatorSHA)
	mount, err := captureQualificationMount(facts.directory)
	if err != nil {
		return qualificationEnvironmentEvidence{}, err
	}
	evidence.Mount = mount
	var name syscall.Utsname
	if err := syscall.Uname(&name); err != nil {
		return qualificationEnvironmentEvidence{}, err
	}
	bootID, err := destructiveBootID()
	if err != nil {
		return qualificationEnvironmentEvidence{}, err
	}
	evidence.Kernel = qualificationKernelEvidence{
		Sysname: utsnameString(name.Sysname[:]), Release: utsnameString(name.Release[:]), Version: utsnameString(name.Version[:]), Machine: utsnameString(name.Machine[:]),
		BootIDSHA256: qualificationSHA256([]byte(bootID)), CommandLineSHA256: qualificationSHA256(readQualificationHostFile("/proc/cmdline", 1<<20)),
		OSReleaseSHA256: qualificationSHA256(readQualificationHostFile("/etc/os-release", 1<<20)),
	}
	devices, err := captureQualificationBlockDevices(mount.MajorMinor)
	if err != nil {
		return qualificationEnvironmentEvidence{}, err
	}
	evidence.Controller.BlockDevices = devices
	hostname, _ := os.Hostname()
	hostIdentity := append([]byte("hostname\x00"+hostname+"\x00machine-id\x00"), readQualificationHostFile("/etc/machine-id", 4096)...)
	hostIdentity = append(hostIdentity, []byte("\x00product-uuid\x00")...)
	hostIdentity = append(hostIdentity, readQualificationHostFile("/sys/class/dmi/id/product_uuid", 4096)...)
	evidence.HostOperator.HostIdentitySHA256 = qualificationSHA256(hostIdentity)
	return evidence, nil
}

func captureQualificationMount(target string) (qualificationMountEvidence, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return qualificationMountEvidence{}, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for index, field := range fields {
			if field == "-" {
				separator = index
				break
			}
		}
		if len(fields) < 10 || separator < 6 || separator+3 >= len(fields) {
			return qualificationMountEvidence{}, errors.New("invalid /proc/self/mountinfo record")
		}
		mountPoint, err := decodeMountInfoPath(fields[4])
		if err != nil {
			return qualificationMountEvidence{}, err
		}
		if filepath.Clean(mountPoint) != filepath.Clean(target) {
			continue
		}
		mountID, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return qualificationMountEvidence{}, errors.New("invalid mount ID")
		}
		parentID, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return qualificationMountEvidence{}, errors.New("invalid parent mount ID")
		}
		root, err := decodeMountInfoPath(fields[3])
		if err != nil {
			return qualificationMountEvidence{}, err
		}
		source, err := decodeMountInfoPath(fields[separator+2])
		if err != nil {
			return qualificationMountEvidence{}, err
		}
		return qualificationMountEvidence{
			MountID: mountID, ParentID: parentID, MajorMinor: fields[2], Root: root, MountPoint: mountPoint,
			MountOptions: qualificationSortedCSV(fields[5]), Filesystem: fields[separator+1], MountSource: source,
			SuperOptions: qualificationSortedCSV(fields[separator+3]),
		}, nil
	}
	if err := scanner.Err(); err != nil {
		return qualificationMountEvidence{}, err
	}
	return qualificationMountEvidence{}, errors.New("target mount is absent from /proc/self/mountinfo")
}

func captureQualificationBlockDevices(initial string) ([]qualificationBlockDeviceEvidence, error) {
	pending := []string{initial}
	seen := make(map[string]struct{})
	var result []qualificationBlockDeviceEvidence
	for len(pending) != 0 {
		majorMinor := pending[0]
		pending = pending[1:]
		if _, exists := seen[majorMinor]; exists {
			continue
		}
		seen[majorMinor] = struct{}{}
		link := filepath.Join("/sys/dev/block", majorMinor)
		resolved, err := filepath.EvalSymlinks(link)
		if err != nil {
			return nil, fmt.Errorf("resolve block device %s: %w", majorMinor, err)
		}
		resolved = filepath.Clean(resolved)
		if !strings.HasPrefix(resolved, "/sys/devices/") {
			return nil, fmt.Errorf("block device %s escapes /sys/devices", majorMinor)
		}
		queue := filepath.Join(resolved, "queue")
		if info, err := os.Stat(queue); err != nil || !info.IsDir() {
			parent := filepath.Dir(resolved)
			if info, err := os.Stat(filepath.Join(parent, "queue")); err == nil && info.IsDir() {
				queue = filepath.Join(parent, "queue")
				if parentDev := strings.TrimSpace(string(readQualificationHostFile(filepath.Join(parent, "dev"), 128))); qualificationMajorMinor(parentDev) {
					pending = append(pending, parentDev)
				}
			}
		}
		device := qualificationBlockDeviceEvidence{
			MajorMinor: majorMinor, SysfsPath: resolved, SizeSectors: qualificationSysfsUint(resolved, "size"),
			LogicalBlockBytes: qualificationSysfsUint(queue, "logical_block_size"), PhysicalBlockBytes: qualificationSysfsUint(queue, "physical_block_size"),
			WriteCache: qualificationSysfsValue(queue, "write_cache"), FUA: qualificationSysfsValue(queue, "fua"),
			Rotational: qualificationSysfsValue(queue, "rotational"), Scheduler: qualificationSysfsValue(queue, "scheduler"),
			DiscardGranularity: qualificationSysfsValue(queue, "discard_granularity"), StableWrites: qualificationSysfsValue(queue, "stable_writes"),
		}
		model := []byte{}
		for _, path := range []string{"device/vendor", "device/model", "device/rev", "device/serial", "wwid"} {
			model = append(model, readQualificationHostFile(filepath.Join(resolved, path), 4096)...)
			model = append(model, 0)
		}
		device.DeviceModelSHA256 = qualificationSHA256(model)
		device.DMUUIDSHA256 = qualificationSHA256(readQualificationHostFile(filepath.Join(resolved, "dm/uuid"), 4096))
		result = append(result, device)
		slaves, err := os.ReadDir(filepath.Join(resolved, "slaves"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		for _, slave := range slaves {
			if slave.Type()&os.ModeSymlink == 0 {
				return nil, errors.New("sysfs block-device slave is not a symbolic link")
			}
			slavePath, err := filepath.EvalSymlinks(filepath.Join(resolved, "slaves", slave.Name()))
			if err != nil {
				return nil, err
			}
			slaveDev := strings.TrimSpace(string(readQualificationHostFile(filepath.Join(slavePath, "dev"), 128)))
			if !qualificationMajorMinor(slaveDev) {
				return nil, errors.New("sysfs block-device slave has invalid device identity")
			}
			pending = append(pending, slaveDev)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MajorMinor < result[j].MajorMinor })
	return result, nil
}

func qualificationSortedCSV(value string) []string {
	values := strings.Split(value, ",")
	sort.Strings(values)
	return values
}

func qualificationSysfsValue(directory, name string) string {
	value := strings.TrimSpace(string(readQualificationHostFile(filepath.Join(directory, name), 4096)))
	if value == "" {
		return "unavailable"
	}
	return value
}

func qualificationSysfsUint(directory, name string) uint64 {
	value, _ := strconv.ParseUint(strings.TrimSpace(string(readQualificationHostFile(filepath.Join(directory, name), 128))), 10, 64)
	return value
}

func readQualificationHostFile(path string, maximum int64) []byte {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maximum {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) > maximum {
		return nil
	}
	return raw
}

func utsnameString(value []int8) string {
	bytes := make([]byte, 0, len(value))
	for _, character := range value {
		if character == 0 {
			break
		}
		bytes = append(bytes, byte(character))
	}
	return string(bytes)
}
