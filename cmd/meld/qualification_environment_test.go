package main

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestQualificationEnvironmentEvidenceBindsRevisionVolumeControllerAndTime(t *testing.T) {
	durability, _ := qualificationFixtures()
	root := t.TempDir()
	environment, _, _ := qualificationEnvironmentFixture(t, root, durability, durability.StartedAt.Add(-time.Minute), "ipmi-chassis-power-cycle")
	if err := validateQualificationEnvironmentAgainstDurability(environment, durability, qualificationTestRevision, "ipmi-chassis-power-cycle", durability.StartedAt); err != nil {
		t.Fatalf("valid environment rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*qualificationEnvironmentEvidence)
		want   string
	}{
		{name: "dirty build", mutate: func(value *qualificationEnvironmentEvidence) { value.BuildModified = true }, want: "identity"},
		{name: "wrong revision", mutate: func(value *qualificationEnvironmentEvidence) { value.BuildRevision = strings.Repeat("f", 40) }, want: "identity"},
		{name: "forged volume token", mutate: func(value *qualificationEnvironmentEvidence) { value.Volume.DestructiveToken += "x" }, want: "token"},
		{name: "controller drift", mutate: func(value *qualificationEnvironmentEvidence) { value.Controller.Method = "pdu-power-cycle" }, want: "controller"},
		{name: "missing controller attestation key", mutate: func(value *qualificationEnvironmentEvidence) { value.Controller.AttestationPublicKeySHA256 = "" }, want: "attestation key"},
		{name: "missing controller target identity", mutate: func(value *qualificationEnvironmentEvidence) { value.Controller.PowerTargetIdentitySHA256 = "" }, want: "target identity"},
		{name: "target substitution", mutate: func(value *qualificationEnvironmentEvidence) {
			value.Volume.Device += 2
			value.Volume.DestructiveToken = destructiveVolumeToken(destructiveVolumeFacts{
				directory: value.Volume.Directory, device: value.Volume.Device, filesystemType: value.Volume.FilesystemType,
				filesystemName: value.Volume.FilesystemName, blockSize: value.Volume.BlockSize, totalBytes: value.Volume.TotalBytes,
			})
		}, want: "target volume"},
		{name: "late capture", mutate: func(value *qualificationEnvironmentEvidence) {
			value.CapturedAt = durability.StartedAt.Add(time.Second)
		}, want: "pre-campaign"},
		{name: "unordered mount options", mutate: func(value *qualificationEnvironmentEvidence) { value.Mount.MountOptions = []string{"sync", "rw"} }, want: "mount"},
		{name: "root operator", mutate: func(value *qualificationEnvironmentEvidence) { value.HostOperator.EffectiveUID = 0 }, want: "host/operator"},
		{name: "missing cache policy", mutate: func(value *qualificationEnvironmentEvidence) { value.Controller.BlockDevices[0].WriteCache = "" }, want: "block device"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := environment
			changed.Mount.MountOptions = append([]string(nil), environment.Mount.MountOptions...)
			changed.Controller.BlockDevices = append([]qualificationBlockDeviceEvidence(nil), environment.Controller.BlockDevices...)
			test.mutate(&changed)
			if err := validateQualificationEnvironmentAgainstDurability(changed, durability, qualificationTestRevision, "ipmi-chassis-power-cycle", durability.StartedAt); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestQualificationEnvironmentCaptureFailsClosedOffLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("non-Linux contract test")
	}
	_, err := captureQualificationEnvironment(destructiveVolumeFacts{}, qualificationTestRevision, "ipmi-chassis-power-cycle", "/missing", 1, strings.Repeat("a", 64))
	if err == nil || !strings.Contains(err.Error(), "only on Linux") {
		t.Fatalf("error=%v", err)
	}
}
