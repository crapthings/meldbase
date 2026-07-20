package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/integrations/anchorhttp"
)

func TestAnchorQualificationKeygenUsesExclusivePrivateKey(t *testing.T) {
	directory := t.TempDir()
	privatePath := filepath.Join(directory, "signing.key")
	publicPath := filepath.Join(directory, "verification.pub")
	var output bytes.Buffer
	if err := runAnchorQualification([]string{"keygen", "--private", privatePath, "--public", publicPath}, &output, &output); err != nil {
		t.Fatal(err)
	}
	privateInfo, err := os.Stat(privatePath)
	if err != nil || privateInfo.Mode().Perm() != 0o600 {
		t.Fatalf("private key mode=%v err=%v", privateInfo.Mode(), err)
	}
	privateKey, err := loadAnchorQualificationPrivateKey(privatePath)
	if err != nil || len(privateKey) != ed25519.PrivateKeySize {
		t.Fatalf("private key size=%d err=%v", len(privateKey), err)
	}
	publicKey, err := loadAnchorQualificationPublicKey(publicPath)
	if err != nil || !privateKey.Public().(ed25519.PublicKey).Equal(publicKey) {
		t.Fatalf("public key size=%d err=%v", len(publicKey), err)
	}
	if err := runAnchorQualification([]string{"keygen", "--private", privatePath, "--public", publicPath}, &output, &output); err == nil {
		t.Fatal("qualification keygen overwrote an existing private key")
	}
}

func TestAnchorQualificationSignedCompleteChainRejectsTamperReplayAndWrongKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	publicPath := filepath.Join(directory, "verification.pub")
	if err := writeAnchorQualificationKey(publicPath, publicKey, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	paths := make([]string, len(anchorQualificationPhases))
	var previousRaw []byte
	for index, phase := range anchorQualificationPhases {
		receipt := syntheticAnchorQualificationReceipt(phase, now.Add(time.Duration(index*2)*time.Second), publicKey)
		if index > 0 {
			receipt.PreviousSHA256 = qualificationSHA256(previousRaw)
		}
		if err := signAnchorQualificationReceipt(&receipt, privateKey); err != nil {
			t.Fatal(err)
		}
		if err := validateAnchorQualificationReceipt(receipt); err != nil {
			t.Fatalf("phase %s: %v", phase, err)
		}
		paths[index] = filepath.Join(directory, phase+".json")
		if err := writeJSONExclusiveDurable(paths[index], receipt); err != nil {
			t.Fatal(err)
		}
		previousRaw, err = readQualificationReceipt(paths[index], &anchorQualificationReceipt{})
		if err != nil {
			t.Fatal(err)
		}
	}
	arguments := []string{"verify", "--public-key", publicPath, "--require-complete"}
	for _, path := range paths {
		arguments = append(arguments, "--receipt", path)
	}
	var output bytes.Buffer
	if err := runAnchorQualification(arguments, &output, &output); err != nil || !strings.Contains(output.String(), `"complete":true`) {
		t.Fatalf("complete verify err=%v output=%s", err, output.String())
	}

	tamperedRaw, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	tamperedRaw = bytes.Replace(tamperedRaw, []byte(`"externalEvidenceSha256": "`+strings.Repeat("2", 64)+`"`), []byte(`"externalEvidenceSha256": "`+strings.Repeat("a", 64)+`"`), 1)
	tamperedPath := filepath.Join(directory, "tampered.json")
	if err := os.WriteFile(tamperedPath, tamperedRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{"verify", "--public-key", publicPath, "--receipt", paths[0], "--receipt", tamperedPath}, &output, &output); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("tampered receipt error=%v", err)
	}
	if err := runAnchorQualification([]string{"verify", "--public-key", publicPath, "--receipt", paths[0], "--receipt", paths[0]}, &output, &output); err == nil {
		t.Fatal("replayed phase receipt was accepted")
	}
	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongPath := filepath.Join(directory, "wrong.pub")
	if err := writeAnchorQualificationKey(wrongPath, wrongPublic, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runAnchorQualification([]string{"verify", "--public-key", wrongPath, "--receipt", paths[0]}, &output, &output); err == nil {
		t.Fatal("receipt verified with another public key")
	}
}

func syntheticAnchorQualificationReceipt(phase string, started time.Time, publicKey ed25519.PublicKey) anchorQualificationReceipt {
	databaseSequence, databaseGeneration := uint64(2), uint64(3)
	anchorSequence, anchorGeneration := databaseSequence, databaseGeneration
	available, unavailable := uint64(3), uint64(0)
	quorumLoad, relation, opened, anchorExists := "succeeded", "equal", "not-attempted", true
	switch phase {
	case "degraded":
		databaseSequence, databaseGeneration, anchorSequence, anchorGeneration = 3, 4, 3, 4
		available, unavailable = 2, 1
	case "minority":
		databaseSequence, databaseGeneration, anchorSequence, anchorGeneration = 4, 5, 0, 0
		available, unavailable, quorumLoad, relation, anchorExists = 1, 2, "unavailable", "unavailable", false
	case "recovered":
		databaseSequence, databaseGeneration, anchorSequence, anchorGeneration, opened = 4, 5, 4, 5, "succeeded"
	case "rollback-rejected":
		databaseSequence, databaseGeneration, anchorSequence, anchorGeneration = 2, 3, 4, 5
		relation, opened = "anchor-ahead", "rollback-rejected"
	}
	members := make([]anchorQualificationMember, 3)
	for index := range members {
		state := anchorhttp.ReplicaAvailable
		exists := true
		memberSequence, memberGeneration := anchorSequence, anchorGeneration
		if uint64(index) >= available {
			state, exists, memberSequence, memberGeneration = anchorhttp.ReplicaUnavailable, false, 0, 0
		} else if phase == "minority" {
			memberSequence, memberGeneration = 3, 4
		}
		members[index] = anchorQualificationMember{
			MemberID: "member-" + string(rune('a'+index)), EndpointSHA256: strings.Repeat(string(rune('a'+index)), 64), State: string(state), Exists: exists,
			MinimumCommitSequence: memberSequence, MinimumGeneration: memberGeneration,
		}
		if exists {
			members[index].DatabaseIDHex = strings.Repeat("01", 16)
		}
	}
	return anchorQualificationReceipt{
		SchemaVersion: anchorQualificationSchema, ProtocolVersion: anchorhttp.ProtocolVersion, Phase: phase, RunID: strings.Repeat("ab", 16),
		GOOS: "linux", GOARCH: "amd64", GoVersion: "go1.23", StartedAt: started, FinishedAt: started.Add(time.Second),
		ConfigurationID: strings.Repeat("c", 64), ExternalEvidenceSHA256: strings.Repeat(string(rune('1'+anchorQualificationPhaseIndex(phase))), 64), Replicas: 3, Quorum: 2, Members: members, AvailableMembers: available, UnavailableMembers: unavailable,
		QuorumLoad: quorumLoad, AnchorExists: anchorExists, AnchorSequence: anchorSequence, AnchorGeneration: anchorGeneration,
		Database:         meldbase.VerificationReport{SchemaVersion: 3, Verified: true, Format: meldbase.StorageFormatCurrent, Revision: 3, DatabaseIDHex: strings.Repeat("01", 16), MetaGeneration: databaseGeneration, CommitSequence: databaseSequence, IndexContentsVerified: true, IndexBuildContentsVerified: true, SHA256: strings.Repeat("d", 64)},
		DatabaseRelation: relation, DatabaseOpen: opened, Passed: true, SigningPublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}
}
