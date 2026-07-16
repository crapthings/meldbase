package qualification

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const soakTestRevision = "0123456789abcdef0123456789abcdef01234567"

func TestValidateSoakOptionsProfilesAndProvenance(t *testing.T) {
	base := SoakOptions{
		TargetDirectory: t.TempDir(), Profile: "custom", Seconds: 1, Documents: 100, Reopens: 1,
		Volume: VolumeIdentity{Device: 1, FilesystemType: "0xef53", FilesystemName: "ext-family", BlockSize: 4096},
	}
	if err := ValidateSoakOptions(base); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*SoakOptions)
		want   string
	}{
		{name: "unknown profile", mutate: func(value *SoakOptions) { value.Profile = "nightly" }, want: "unknown"},
		{name: "short", mutate: func(value *SoakOptions) { value.Seconds = 0 }, want: "1 second"},
		{name: "few documents", mutate: func(value *SoakOptions) { value.Documents = 99 }, want: "100"},
		{name: "missing volume", mutate: func(value *SoakOptions) { value.Volume = VolumeIdentity{} }, want: "target-volume"},
		{name: "invalid source", mutate: func(value *SoakOptions) { value.SourceRevision = "main" }, want: "source revision"},
		{name: "fast progress", mutate: func(value *SoakOptions) { value.ProgressInterval = 99 * time.Millisecond }, want: "progress interval"},
		{name: "slow progress", mutate: func(value *SoakOptions) { value.ProgressInterval = 5*time.Minute + time.Nanosecond }, want: "progress interval"},
		{name: "release floor", mutate: func(value *SoakOptions) {
			value.Profile, value.Seconds, value.Documents, value.Reopens = "release", 14_399, 10_000, 12
		}, want: "at least 4 hours"},
		{name: "release dirty", mutate: func(value *SoakOptions) {
			value.Profile, value.Seconds, value.Documents, value.Reopens = "release", 14_400, 10_000, 12
			value.SourceRevision, value.BuildRevision, value.BuildModified = soakTestRevision, soakTestRevision, true
		}, want: "clean binary"},
		{name: "release mismatch", mutate: func(value *SoakOptions) {
			value.Profile, value.Seconds, value.Documents, value.Reopens = "release", 14_400, 10_000, 12
			value.SourceRevision, value.BuildRevision = soakTestRevision, strings.Repeat("f", 40)
		}, want: "claimed source"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.mutate(&value)
			err := ValidateSoakOptions(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	cleanRelease := base
	cleanRelease.Profile, cleanRelease.Seconds, cleanRelease.Documents, cleanRelease.Reopens = "release", 14_400, 10_000, 12
	cleanRelease.SourceRevision, cleanRelease.BuildRevision = soakTestRevision, soakTestRevision
	err := ValidateSoakOptions(cleanRelease)
	if RaceEnabled && err != nil {
		t.Fatalf("race-enabled clean release rejected: %v", err)
	}
	if !RaceEnabled && (err == nil || !strings.Contains(err.Error(), "race-enabled")) {
		t.Fatalf("non-race release error=%v", err)
	}
}

func TestRunStorageSoakExercisesAndCleansTarget(t *testing.T) {
	target := t.TempDir()
	progressEvents := make([]SoakProgress, 0, 16)
	receipt, err := RunStorageSoak(context.Background(), SoakOptions{
		TargetDirectory: target, Profile: "custom", Seconds: 1, Documents: 100, Reopens: 1,
		BuildRevision: soakTestRevision, BuildModified: true,
		Volume:           VolumeIdentity{Device: 42, FilesystemType: "0xef53", FilesystemName: "ext-family", BlockSize: 4096},
		ProgressInterval: 100 * time.Millisecond,
		Progress:         func(progress SoakProgress) { progressEvents = append(progressEvents, progress) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.SchemaVersion != 4 || receipt.Profile != "custom" || receipt.RaceEnabled != RaceEnabled ||
		receipt.BuildRevision != soakTestRevision || !receipt.BuildModified || receipt.CompletedReopens != 1 || len(receipt.Phases) != 1 ||
		receipt.ConcurrentDuration < time.Second || receipt.Phases[0].ConcurrentDuration < time.Second ||
		receipt.Writes == 0 || receipt.Writes > 20 || receipt.SnapshotReads == 0 || receipt.IndexBuildBatches == 0 || receipt.ReclamationAttempts == 0 ||
		!receipt.SemanticIndexes || !receipt.SemanticIndexBuilds || !receipt.FinalIndexBuildAbsent || !receipt.FreeSpaceValid ||
		len(receipt.FinalFileSHA256) != 64 || receipt.ActualDuration <= 0 {
		t.Fatalf("receipt=%+v", receipt)
	}
	drift := receipt.FinishedAt.Sub(receipt.StartedAt) - receipt.ActualDuration
	if drift < 0 {
		drift = -drift
	}
	if drift > 5*time.Second {
		t.Fatalf("receipt clock drift=%s receipt=%+v", drift, receipt)
	}
	if len(progressEvents) < 5 || progressEvents[0].Stage != SoakProgressStarted ||
		progressEvents[len(progressEvents)-1].Stage != SoakProgressComplete {
		t.Fatalf("progress=%+v", progressEvents)
	}
	wantStages := []SoakProgressStage{
		SoakProgressStarted,
		SoakProgressPhaseRunning,
		SoakProgressPhaseVerifying,
		SoakProgressPhaseVerified,
		SoakProgressShadowVerifying,
		SoakProgressShadowVerified,
		SoakProgressFinalVerifying,
		SoakProgressComplete,
	}
	nextStage := 0
	var previousConcurrent time.Duration
	var previousWrites uint64
	for _, progress := range progressEvents {
		if progress.ConcurrentDuration < previousConcurrent || progress.Writes < previousWrites ||
			progress.Phase < 0 || progress.Phase > progress.TotalPhases {
			t.Fatalf("non-monotonic progress=%+v previousConcurrent=%s previousWrites=%d", progress, previousConcurrent, previousWrites)
		}
		previousConcurrent, previousWrites = progress.ConcurrentDuration, progress.Writes
		if nextStage < len(wantStages) && progress.Stage == wantStages[nextStage] {
			nextStage++
		}
	}
	if nextStage != len(wantStages) {
		t.Fatalf("progress stages out of order: matched=%d want=%v progress=%+v", nextStage, wantStages, progressEvents)
	}
	complete := progressEvents[len(progressEvents)-1]
	if complete.ConcurrentDuration != receipt.ConcurrentDuration || complete.Writes != receipt.Writes ||
		complete.SnapshotReads != receipt.SnapshotReads || complete.IndexBuildBatches != receipt.IndexBuildBatches ||
		complete.ReclamationAttempts != receipt.ReclamationAttempts {
		t.Fatalf("complete progress=%+v receipt=%+v", complete, receipt)
	}
	assertNoSoakDirectory(t, target)
}

func TestRunWithProgressHeartbeatReportsLongOperationAndResult(t *testing.T) {
	heartbeats := 0
	err := runWithProgressHeartbeat(10*time.Millisecond, func() {
		heartbeats++
	}, func() error {
		time.Sleep(35 * time.Millisecond)
		return errors.New("verification failed")
	})
	if err == nil || err.Error() != "verification failed" {
		t.Fatalf("error=%v", err)
	}
	if heartbeats < 2 {
		t.Fatalf("heartbeats=%d", heartbeats)
	}
}

func TestRunStorageSoakCancellationCleansTarget(t *testing.T) {
	target := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := RunStorageSoak(ctx, SoakOptions{
		TargetDirectory: target, Profile: "custom", Seconds: 2, Documents: 100, Reopens: 1,
		Volume: VolumeIdentity{Device: 42, FilesystemType: "0xef53", FilesystemName: "ext-family", BlockSize: 4096},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	assertNoSoakDirectory(t, target)
}

func assertNoSoakDirectory(t *testing.T, target string) {
	t.Helper()
	leftovers, err := filepath.Glob(filepath.Join(target, ".meldbase-storage-soak-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("leftovers=%v err=%v", leftovers, err)
	}
}
