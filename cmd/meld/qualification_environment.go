package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const qualificationEnvironmentSchema uint32 = 2

type qualificationEnvironmentEvidence struct {
	SchemaVersion  uint32                            `json:"schemaVersion"`
	SourceRevision string                            `json:"sourceRevision"`
	BuildRevision  string                            `json:"buildRevision"`
	BuildModified  bool                              `json:"buildModified"`
	CapturedAt     time.Time                         `json:"capturedAt"`
	GOOS           string                            `json:"goos"`
	GOARCH         string                            `json:"goarch"`
	GoVersion      string                            `json:"goVersion"`
	Volume         destructiveVolumeReceipt          `json:"volume"`
	Kernel         qualificationKernelEvidence       `json:"kernel"`
	Mount          qualificationMountEvidence        `json:"mount"`
	Controller     qualificationControllerEvidence   `json:"controller"`
	HostOperator   qualificationHostOperatorEvidence `json:"hostOperator"`
}

type qualificationKernelEvidence struct {
	Sysname           string `json:"sysname"`
	Release           string `json:"release"`
	Version           string `json:"version"`
	Machine           string `json:"machine"`
	BootIDSHA256      string `json:"bootIdSha256"`
	CommandLineSHA256 string `json:"commandLineSha256"`
	OSReleaseSHA256   string `json:"osReleaseSha256"`
}

type qualificationMountEvidence struct {
	MountID      uint64   `json:"mountId"`
	ParentID     uint64   `json:"parentId"`
	MajorMinor   string   `json:"majorMinor"`
	Root         string   `json:"root"`
	MountPoint   string   `json:"mountPoint"`
	MountOptions []string `json:"mountOptions"`
	Filesystem   string   `json:"filesystem"`
	MountSource  string   `json:"mountSource"`
	SuperOptions []string `json:"superOptions"`
}

type qualificationControllerEvidence struct {
	Method                     string                             `json:"method"`
	SysfsRoot                  string                             `json:"sysfsRoot"`
	AttestationPublicKeySHA256 string                             `json:"attestationPublicKeySha256,omitempty"`
	PowerTargetIdentitySHA256  string                             `json:"powerTargetIdentitySha256,omitempty"`
	BlockDevices               []qualificationBlockDeviceEvidence `json:"blockDevices"`
}

type qualificationBlockDeviceEvidence struct {
	MajorMinor         string `json:"majorMinor"`
	SysfsPath          string `json:"sysfsPath"`
	SizeSectors        uint64 `json:"sizeSectors"`
	LogicalBlockBytes  uint64 `json:"logicalBlockBytes"`
	PhysicalBlockBytes uint64 `json:"physicalBlockBytes"`
	WriteCache         string `json:"writeCache"`
	FUA                string `json:"fua"`
	Rotational         string `json:"rotational"`
	Scheduler          string `json:"scheduler"`
	DiscardGranularity string `json:"discardGranularity"`
	StableWrites       string `json:"stableWrites"`
	DeviceModelSHA256  string `json:"deviceModelSha256"`
	DMUUIDSHA256       string `json:"dmUuidSha256"`
}

type qualificationHostOperatorEvidence struct {
	EffectiveUID           int    `json:"effectiveUid"`
	HostIdentitySHA256     string `json:"hostIdentitySha256"`
	OperatorEvidencePath   string `json:"operatorEvidencePath"`
	OperatorEvidenceBytes  uint64 `json:"operatorEvidenceBytes"`
	OperatorEvidenceSHA256 string `json:"operatorEvidenceSha256"`
}

func runQualificationEnvironmentCapture(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-environment-capture", flag.ContinueOnError)
	flags.SetOutput(stderr)
	targetPath := flags.String("dir", "", "root of the empty disposable mounted target volume")
	controlPath := flags.String("control-dir", "", "existing evidence directory on a different device")
	controllerMethod := flags.String("controller-method", "", "external destructive controller method used for the campaign")
	controllerPublicKeyPath := flags.String("controller-public-key", "", "trusted controller-agent Ed25519 public key for physical methods")
	controllerTargetIdentity := flags.String("controller-target-identity-sha256", "", "pre-approved hash of the physical server/chassis/outlet identity")
	operatorEvidencePath := flags.String("operator-evidence", "", "nonempty secured operator authorization/change record")
	sourceRevision := flags.String("source-revision", "", "exact 40- or 64-hex release revision")
	outputPath := flags.String("out", "", "new schema-2 qualification environment evidence")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *targetPath == "" || *controlPath == "" || !qualificationPowerMethod(*controllerMethod) || *operatorEvidencePath == "" ||
		!validDurabilitySourceRevision(*sourceRevision) || *outputPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-environment-capture requires target/control directories, controller method, operator evidence, source revision and --out")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if buildRevision != *sourceRevision || buildModified {
		return errors.New("qualification environment capture requires a clean binary built from the qualified revision")
	}
	facts, err := inspectDestructiveVolume(*targetPath, *controlPath)
	if err != nil {
		return err
	}
	if err := validateDestructiveVolumeFacts(facts); err != nil {
		return err
	}
	operatorPath, err := filepath.Abs(filepath.Clean(*operatorEvidencePath))
	if err != nil {
		return err
	}
	operatorInfo, err := os.Lstat(operatorPath)
	if err != nil || operatorInfo.Mode()&os.ModeSymlink != 0 || !operatorInfo.Mode().IsRegular() || operatorInfo.Size() <= 0 || operatorInfo.Size() > qualificationReceiptMaxBytes {
		return errors.New("operator evidence must be a nonempty regular non-symlink file no larger than 1 MiB")
	}
	operatorSHA, err := hashRegularFile(operatorPath, qualificationReceiptMaxBytes)
	if err != nil {
		return err
	}
	evidence, err := captureQualificationEnvironment(facts, *sourceRevision, *controllerMethod, operatorPath, uint64(operatorInfo.Size()), operatorSHA)
	if err != nil {
		return err
	}
	if qualificationPhysicalPowerMethod(*controllerMethod) {
		if *controllerPublicKeyPath == "" {
			return errors.New("physical controller environment capture requires --controller-public-key")
		}
		publicPath, err := filepath.Abs(filepath.Clean(*controllerPublicKeyPath))
		if err != nil {
			return err
		}
		publicKey, err := loadAnchorQualificationPublicKey(publicPath)
		if err != nil {
			return fmt.Errorf("controller public key: %w", err)
		}
		evidence.Controller.AttestationPublicKeySHA256 = qualificationSHA256(publicKey)
		if !qualificationHexDigest(*controllerTargetIdentity) {
			return errors.New("physical controller environment capture requires --controller-target-identity-sha256")
		}
		evidence.Controller.PowerTargetIdentitySHA256 = *controllerTargetIdentity
	} else if *controllerPublicKeyPath != "" || *controllerTargetIdentity != "" {
		return errors.New("QEMU controller environment must not supply physical controller identity")
	}
	if err := validateQualificationEnvironmentEvidence(evidence, *sourceRevision, ""); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(*outputPath, evidence); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(evidence)
}

func validateQualificationEnvironmentEvidence(evidence qualificationEnvironmentEvidence, revision, controllerMethod string) error {
	if evidence.SchemaVersion != qualificationEnvironmentSchema || evidence.SourceRevision != revision || evidence.BuildRevision != revision || evidence.BuildModified ||
		evidence.CapturedAt.IsZero() || evidence.GOOS != "linux" || evidence.GOARCH == "" || evidence.GoVersion == "" ||
		evidence.Volume.SchemaVersion != destructiveVolumeSchema || !evidence.Volume.Eligible || evidence.Volume.GOOS != "linux" || evidence.Volume.Device == 0 ||
		evidence.Volume.ControlDevice == 0 || evidence.Volume.Device == evidence.Volume.ControlDevice || evidence.Volume.BlockSize == 0 || evidence.Volume.TotalBytes == 0 ||
		evidence.Volume.TotalBytes < destructiveVolumeMinBytes || evidence.Volume.TotalBytes > destructiveVolumeMaxBytes ||
		evidence.Volume.AvailableBytes > evidence.Volume.TotalBytes || evidence.Volume.ControlAvailableBytes < destructiveControlMinBytes ||
		!qualificationProductionFilesystem(evidence.Volume.FilesystemName) || !filepath.IsAbs(evidence.Volume.Directory) || evidence.Volume.DestructiveToken == "" {
		return errors.New("qualification environment identity, build or volume evidence is invalid")
	}
	if destructiveVolumeToken(destructiveVolumeFacts{
		directory: evidence.Volume.Directory, device: evidence.Volume.Device, filesystemType: evidence.Volume.FilesystemType,
		filesystemName: evidence.Volume.FilesystemName, blockSize: evidence.Volume.BlockSize, totalBytes: evidence.Volume.TotalBytes,
	}) != evidence.Volume.DestructiveToken {
		return errors.New("qualification environment destructive volume token is invalid")
	}
	if !qualificationEvidenceText(evidence.Kernel.Sysname, 128) || !qualificationEvidenceText(evidence.Kernel.Release, 256) ||
		!qualificationEvidenceText(evidence.Kernel.Version, 512) || !qualificationEvidenceText(evidence.Kernel.Machine, 128) ||
		!qualificationHexDigest(evidence.Kernel.BootIDSHA256) || !qualificationHexDigest(evidence.Kernel.CommandLineSHA256) || !qualificationHexDigest(evidence.Kernel.OSReleaseSHA256) {
		return errors.New("qualification environment kernel evidence is incomplete")
	}
	mount := evidence.Mount
	if mount.MountID == 0 || mount.ParentID == 0 || !qualificationMajorMinor(mount.MajorMinor) || mount.MountPoint != evidence.Volume.Directory ||
		!filepath.IsAbs(mount.Root) || !filepath.IsAbs(mount.MountPoint) || !qualificationEvidenceText(mount.Filesystem, 128) ||
		!qualificationEvidenceText(mount.MountSource, 1024) || !qualificationSortedEvidenceStrings(mount.MountOptions, 128) ||
		!qualificationSortedEvidenceStrings(mount.SuperOptions, 128) {
		return errors.New("qualification environment mount evidence is incomplete")
	}
	mountDevice, err := qualificationLinuxDeviceNumber(mount.MajorMinor)
	if err != nil || mountDevice != evidence.Volume.Device || qualificationMountFilesystemName(mount.Filesystem) != evidence.Volume.FilesystemName {
		return errors.New("qualification environment mount device or filesystem differs from the target volume")
	}
	controller := evidence.Controller
	if !qualificationPowerMethod(controller.Method) || (controllerMethod != "" && controller.Method != controllerMethod) || controller.SysfsRoot != "/sys" ||
		len(controller.BlockDevices) == 0 || len(controller.BlockDevices) > 128 {
		return errors.New("qualification environment controller method or block-device chain is invalid")
	}
	if qualificationPhysicalPowerMethod(controller.Method) {
		if !qualificationHexDigest(controller.AttestationPublicKeySHA256) || !qualificationHexDigest(controller.PowerTargetIdentitySHA256) {
			return errors.New("qualification environment physical controller attestation key or target identity is incomplete")
		}
	} else if controller.AttestationPublicKeySHA256 != "" || controller.PowerTargetIdentitySHA256 != "" {
		return errors.New("qualification environment QEMU controller has an unexpected physical attestation key")
	}
	seenDevices := make(map[string]struct{}, len(controller.BlockDevices))
	previousDevice := ""
	foundMountDevice := false
	for ordinal, device := range controller.BlockDevices {
		if !qualificationMajorMinor(device.MajorMinor) || !strings.HasPrefix(device.SysfsPath, "/sys/devices/") ||
			device.SizeSectors == 0 || device.LogicalBlockBytes == 0 || device.PhysicalBlockBytes == 0 ||
			!qualificationEvidenceText(device.WriteCache, 128) || !qualificationEvidenceText(device.FUA, 128) ||
			!qualificationEvidenceText(device.Rotational, 128) || !qualificationEvidenceText(device.Scheduler, 512) ||
			!qualificationEvidenceText(device.DiscardGranularity, 128) || !qualificationEvidenceText(device.StableWrites, 128) ||
			!qualificationHexDigest(device.DeviceModelSHA256) || !qualificationHexDigest(device.DMUUIDSHA256) {
			return fmt.Errorf("qualification environment block device %d is incomplete", ordinal+1)
		}
		if _, duplicate := seenDevices[device.MajorMinor]; duplicate {
			return fmt.Errorf("qualification environment repeats block device %q", device.MajorMinor)
		}
		if device.MajorMinor <= previousDevice {
			return errors.New("qualification environment block-device chain is not in canonical order")
		}
		seenDevices[device.MajorMinor] = struct{}{}
		previousDevice = device.MajorMinor
		foundMountDevice = foundMountDevice || device.MajorMinor == mount.MajorMinor
	}
	if !foundMountDevice {
		return errors.New("qualification environment block-device chain omits the mounted device")
	}
	host := evidence.HostOperator
	if host.EffectiveUID == 0 || !qualificationHexDigest(host.HostIdentitySHA256) || !filepath.IsAbs(host.OperatorEvidencePath) ||
		host.OperatorEvidenceBytes == 0 || host.OperatorEvidenceBytes > qualificationReceiptMaxBytes || !qualificationHexDigest(host.OperatorEvidenceSHA256) {
		return errors.New("qualification environment host/operator evidence is incomplete")
	}
	return nil
}

func validateQualificationEnvironmentAgainstDurability(evidence qualificationEnvironmentEvidence, durability durabilityCheckResult, revision, controllerMethod string, startedAt time.Time) error {
	if err := validateQualificationEnvironmentEvidence(evidence, revision, controllerMethod); err != nil {
		return err
	}
	if evidence.GOARCH != durability.GOARCH || evidence.GoVersion != durability.GoVersion || evidence.Volume.Directory != durability.Directory || evidence.Volume.Device != durability.Device ||
		evidence.Volume.FilesystemType != durability.FilesystemType || evidence.Volume.FilesystemName != durability.FilesystemName || evidence.Volume.BlockSize != durability.BlockSize ||
		evidence.Volume.TotalBytes != durability.TotalBytes ||
		evidence.CapturedAt.After(startedAt) {
		return errors.New("qualification environment does not bind the campaign runtime, target volume or pre-campaign time")
	}
	return nil
}

func validateQualificationEnvironmentBinding(evidence qualificationEnvironmentEvidence, raw []byte, artifacts verifiedQualificationArtifactIndex,
	durability durabilityCheckResult, destructive qualificationDestructiveRecord, revision, environmentPath string) error {
	if err := validateQualificationEnvironmentAgainstDurability(evidence, durability, revision, destructive.Infrastructure.ControllerMethod, destructive.StartedAt); err != nil {
		return err
	}
	if qualificationSHA256(raw) != destructive.Infrastructure.EnvironmentRecordSHA256 {
		return errors.New("environment record differs from destructive manifest binding")
	}
	if err := requireQualificationArtifactPaths(artifacts, environmentPath); err != nil {
		return err
	}
	if _, err := qualificationArtifactEntryForDigest(artifacts, evidence.HostOperator.OperatorEvidenceSHA256, evidence.HostOperator.OperatorEvidenceBytes); err != nil {
		return fmt.Errorf("operator evidence: %w", err)
	}
	kernelAndMountSHA, err := qualificationEnvironmentSectionSHA256(struct {
		Volume destructiveVolumeReceipt    `json:"volume"`
		Kernel qualificationKernelEvidence `json:"kernel"`
		Mount  qualificationMountEvidence  `json:"mount"`
	}{evidence.Volume, evidence.Kernel, evidence.Mount})
	if err != nil {
		return err
	}
	controllerSHA, err := qualificationEnvironmentSectionSHA256(evidence.Controller)
	if err != nil {
		return err
	}
	hostSHA, err := qualificationEnvironmentSectionSHA256(evidence.HostOperator)
	if err != nil {
		return err
	}
	if kernelAndMountSHA != destructive.Infrastructure.KernelAndMountSHA256 || controllerSHA != destructive.Infrastructure.ControllerPolicySHA256 ||
		hostSHA != destructive.Infrastructure.HostAndOperatorSHA256 {
		return errors.New("environment sections differ from destructive manifest bindings")
	}
	return nil
}

func qualificationEnvironmentSectionSHA256(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return qualificationSHA256(raw), nil
}

func qualificationEvidenceText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == 0 || character == '\r' || character == '\n' {
			return false
		}
	}
	return true
}

func qualificationSortedEvidenceStrings(values []string, maximum int) bool {
	if len(values) == 0 || len(values) > maximum || !sort.StringsAreSorted(values) {
		return false
	}
	previous := ""
	for _, value := range values {
		if value == previous || !qualificationEvidenceText(value, 1024) {
			return false
		}
		previous = value
	}
	return true
}

func qualificationMajorMinor(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, character := range part {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

func qualificationLinuxDeviceNumber(value string) (uint64, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, errors.New("invalid Linux major:minor device identity")
	}
	major, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, err
	}
	minor, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, err
	}
	return (minor & 0xff) | ((major & 0xfff) << 8) | ((minor & ^uint64(0xff)) << 12) | ((major & ^uint64(0xfff)) << 32), nil
}

func qualificationMountFilesystemName(value string) string {
	switch value {
	case "ext2", "ext3", "ext4":
		return "ext-family"
	case "xfs", "btrfs":
		return value
	default:
		return ""
	}
}

func qualificationEnvironmentBase(facts destructiveVolumeFacts, revision, method, operatorPath string, operatorBytes uint64, operatorSHA string) qualificationEnvironmentEvidence {
	buildRevision, buildModified := durabilityBuildIdentity()
	return qualificationEnvironmentEvidence{
		SchemaVersion: qualificationEnvironmentSchema, SourceRevision: revision, BuildRevision: buildRevision, BuildModified: buildModified,
		CapturedAt: time.Now().UTC(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		Volume: destructiveVolumeReceipt{
			SchemaVersion: destructiveVolumeSchema, Eligible: true, Directory: facts.directory, GOOS: runtime.GOOS,
			Device: facts.device, ControlDevice: facts.controlDevice, FilesystemType: facts.filesystemType, FilesystemName: facts.filesystemName,
			BlockSize: facts.blockSize, TotalBytes: facts.totalBytes, AvailableBytes: facts.availableBytes,
			ControlAvailableBytes: facts.controlAvailableBytes, DestructiveToken: destructiveVolumeToken(facts),
		},
		Controller: qualificationControllerEvidence{Method: method, SysfsRoot: "/sys"},
		HostOperator: qualificationHostOperatorEvidence{
			EffectiveUID: os.Geteuid(), OperatorEvidencePath: operatorPath, OperatorEvidenceBytes: operatorBytes, OperatorEvidenceSHA256: operatorSHA,
		},
	}
}
