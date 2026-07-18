package main

import (
	"bytes"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestValidateDestructiveVolumeFactsAcceptsOnlyIndependentDisposableMount(t *testing.T) {
	valid := destructiveVolumeFacts{
		directory: "/mnt/meldbase-destructive", controlDirectory: "/var/lib/meldbase-evidence", device: 42, parentDevice: 1, workDevice: 1, controlDevice: 1,
		filesystemType: "0xef53", filesystemName: "ext-family", blockSize: 4096,
		totalBytes: 1 << 30, availableBytes: 900 << 20, controlAvailableBytes: 1 << 30, effectiveUID: 1000, mountRoot: true, empty: true,
	}
	if err := validateDestructiveVolumeFacts(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*destructiveVolumeFacts)
		want   string
	}{
		{name: "not mount root", mutate: func(f *destructiveVolumeFacts) { f.mountRoot = false }, want: "independently mounted"},
		{name: "same as parent", mutate: func(f *destructiveVolumeFacts) { f.parentDevice = f.device }, want: "independently mounted"},
		{name: "workspace device", mutate: func(f *destructiveVolumeFacts) { f.workDevice = f.device }, want: "workspace"},
		{name: "control device", mutate: func(f *destructiveVolumeFacts) { f.controlDevice = f.device }, want: "different devices"},
		{name: "control full", mutate: func(f *destructiveVolumeFacts) { f.controlAvailableBytes = destructiveControlMinBytes - 1 }, want: "control directory"},
		{name: "root user", mutate: func(f *destructiveVolumeFacts) { f.effectiveUID = 0 }, want: "non-root"},
		{name: "nonempty", mutate: func(f *destructiveVolumeFacts) { f.empty = false }, want: "must be empty"},
		{name: "too small", mutate: func(f *destructiveVolumeFacts) {
			f.totalBytes, f.availableBytes = destructiveVolumeMinBytes-1, destructiveVolumeMinBytes-2
		}, want: "capacity"},
		{name: "too large", mutate: func(f *destructiveVolumeFacts) { f.totalBytes = destructiveVolumeMaxBytes + 1 }, want: "capacity"},
		{name: "unsupported fs", mutate: func(f *destructiveVolumeFacts) { f.filesystemName = "tmpfs" }, want: "production qualification"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			facts := valid
			test.mutate(&facts)
			if err := validateDestructiveVolumeFacts(facts); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v facts=%+v", err, facts)
			}
		})
	}
}

func TestDestructiveVolumeTokenBindsGeometryAndPath(t *testing.T) {
	facts := destructiveVolumeFacts{
		directory: "/mnt/one", device: 42, filesystemType: "0xef53", filesystemName: "ext-family",
		blockSize: 4096, totalBytes: 1 << 30,
	}
	first := destructiveVolumeToken(facts)
	if !strings.HasPrefix(first, "meldbase-enospc-") || first != destructiveVolumeToken(facts) {
		t.Fatalf("token=%q", first)
	}
	facts.directory = "/mnt/two"
	if second := destructiveVolumeToken(facts); second == first {
		t.Fatal("target path did not change destructive token")
	}
	facts.directory, facts.totalBytes = "/mnt/one", 2<<30
	if second := destructiveVolumeToken(facts); second == first {
		t.Fatal("target geometry did not change destructive token")
	}
}

func TestDestructiveVolumeCheckFailsClosedOffLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("off-Linux behavior")
	}
	var output bytes.Buffer
	err := run([]string{"destructive-volume-check", "--dir", filepath.Clean("."), "--control-dir", filepath.Clean(".")}, &output, &output)
	if err == nil || !strings.Contains(err.Error(), "only on Linux") {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}
