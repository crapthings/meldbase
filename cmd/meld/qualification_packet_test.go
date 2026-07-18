package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQualificationReleasePacketSignsAndOfflineVerifierRecomputesEvidence(t *testing.T) {
	fixture := newQualificationPacketFixture(t)
	withQualificationPacketBuild(t, qualificationTestRevision, false)
	packetPath := filepath.Join(t.TempDir(), "release-packet.json")
	arguments := append(qualificationLevelFiveArguments(fixture.durabilityPath, fixture.soakPath, fixture.destructivePath, fixture.artifacts, fixture.anchor),
		"--release-signing-key", fixture.releasePrivatePath, "--out", packetPath)
	var output bytes.Buffer
	if err := run(arguments, &output, &output); err != nil {
		t.Fatalf("sign release packet=%v output=%s", err, output.String())
	}
	var packet qualificationReleasePacket
	if err := json.Unmarshal(output.Bytes(), &packet); err != nil {
		t.Fatal(err)
	}
	if packet.SchemaVersion != qualificationReleasePacketSchema || packet.Qualification.EvidenceLevel != 5 || !packet.Qualification.ProductionQualified ||
		packet.Verifier.BuildRevision != qualificationTestRevision || packet.Verifier.BuildModified || packet.Signature == "" {
		t.Fatalf("packet=%+v", packet)
	}
	if err := verifyQualificationReleasePacket(packet, fixture.releasePublicKey); err != nil {
		t.Fatal(err)
	}
	verifyArguments := qualificationPacketVerifyArguments(packetPath, fixture)
	output.Reset()
	if err := run(verifyArguments, &output, &output); err != nil || !strings.Contains(output.String(), `"passed":true`) {
		t.Fatalf("offline verify=%v output=%s", err, output.String())
	}
	artifactPath := filepath.Join(fixture.artifacts.rootPath, "evidence.bin")
	artifactRaw, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifactPath, []byte("substituted qualification evidence\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(verifyArguments, &output, &output); err == nil || !strings.Contains(err.Error(), "differs from index") {
		t.Fatalf("substituted secured artifact error=%v", err)
	}
	if err := os.WriteFile(artifactPath, artifactRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	indexRaw, err := os.ReadFile(fixture.artifacts.indexPath)
	if err != nil {
		t.Fatal(err)
	}
	var reboundIndex qualificationArtifactIndex
	if err := json.Unmarshal(indexRaw, &reboundIndex); err != nil {
		t.Fatal(err)
	}
	compactIndex, err := json.Marshal(reboundIndex)
	if err != nil {
		t.Fatal(err)
	}
	compactIndexPath := filepath.Join(t.TempDir(), "compact-artifacts-index.json")
	if err := os.WriteFile(compactIndexPath, compactIndex, 0o600); err != nil {
		t.Fatal(err)
	}
	reindexed := append([]string(nil), verifyArguments...)
	reindexed[indexOfArgumentValue(reindexed, "--artifacts-index", 0)] = compactIndexPath
	if err := run(reindexed, &output, &output); err == nil || !strings.Contains(err.Error(), "differs from destructive record binding") {
		t.Fatalf("re-encoded artifact index error=%v", err)
	}
	rebound := qualificationPacketVerifyArguments(packetPath, fixture)
	first := indexOfArgumentValue(rebound, "--anchor-phase-receipt", 0)
	second := indexOfArgumentValue(rebound, "--anchor-phase-receipt", 1)
	rebound[first], rebound[second] = rebound[second], rebound[first]
	if err := run(rebound, &output, &output); err == nil || !strings.Contains(err.Error(), "out of phase order") {
		t.Fatalf("rebound original evidence error=%v", err)
	}
	if err := run(arguments, &output, &output); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Fatalf("release packet output was overwritten: %v", err)
	}
	withQualificationPacketBuild(t, strings.Repeat("5", 40), false)
	if err := run(verifyArguments, &output, &output); err == nil || !strings.Contains(err.Error(), "clean verifier") {
		t.Fatalf("old offline verifier build error=%v", err)
	}
}

func TestQualificationReleasePacketRejectsForgeryWrongKeyOldVerifierAndAnchorKeyReuse(t *testing.T) {
	fixture := newQualificationPacketFixture(t)
	withQualificationPacketBuild(t, qualificationTestRevision, false)
	packetPath := filepath.Join(t.TempDir(), "release-packet.json")
	arguments := append(qualificationLevelFiveArguments(fixture.durabilityPath, fixture.soakPath, fixture.destructivePath, fixture.artifacts, fixture.anchor),
		"--release-signing-key", fixture.releasePrivatePath, "--out", packetPath)
	if err := run(arguments, ioDiscard{}, ioDiscard{}); err != nil {
		t.Fatal(err)
	}
	var packet qualificationReleasePacket
	if _, err := readQualificationReceipt(packetPath, &packet); err != nil {
		t.Fatal(err)
	}
	tampered := packet
	tampered.SignedAt = tampered.SignedAt.Add(time.Second)
	if err := verifyQualificationReleasePacket(tampered, fixture.releasePublicKey); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("tampered packet signature error=%v", err)
	}

	forged := packet
	forged.Qualification.AnchorHistoryReceiptSHA256 = strings.Repeat("7", 64)
	if err := signQualificationReleasePacket(&forged, fixture.releasePrivateKey); err != nil {
		t.Fatal(err)
	}
	forgedPath, _ := writeQualificationFixture(t, "forged-release-packet.json", forged)
	var output bytes.Buffer
	if err := run(qualificationPacketVerifyArguments(forgedPath, fixture), &output, &output); err == nil || !strings.Contains(err.Error(), "differs from recomputed") {
		t.Fatalf("re-signed forged packet error=%v", err)
	}

	oldVerifier := packet
	oldVerifier.Verifier.BuildRevision = strings.Repeat("6", 40)
	if err := signQualificationReleasePacket(&oldVerifier, fixture.releasePrivateKey); err != nil {
		t.Fatal(err)
	}
	if err := verifyQualificationReleasePacket(oldVerifier, fixture.releasePublicKey); err == nil || !strings.Contains(err.Error(), "verifier provenance") {
		t.Fatalf("old verifier packet error=%v", err)
	}

	wrongPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongPublicPath := filepath.Join(t.TempDir(), "wrong-release.pub")
	if err := writeAnchorQualificationKey(wrongPublicPath, wrongPublic, 0o644); err != nil {
		t.Fatal(err)
	}
	wrongKeyArguments := qualificationPacketVerifyArguments(packetPath, fixture)
	wrongKeyArguments[4] = wrongPublicPath
	if err := run(wrongKeyArguments, &output, &output); err == nil || !strings.Contains(err.Error(), "public key differs") {
		t.Fatalf("wrong release public key error=%v", err)
	}

	anchorPrivatePath := filepath.Join(t.TempDir(), "anchor-private.key")
	if err := writeAnchorQualificationKey(anchorPrivatePath, fixture.anchor.privateKey, 0o600); err != nil {
		t.Fatal(err)
	}
	reusedKeyArguments := append(qualificationLevelFiveArguments(fixture.durabilityPath, fixture.soakPath, fixture.destructivePath, fixture.artifacts, fixture.anchor),
		"--release-signing-key", anchorPrivatePath, "--out", filepath.Join(t.TempDir(), "never.json"))
	if err := run(reusedKeyArguments, &output, &output); err == nil || !strings.Contains(err.Error(), "independent") {
		t.Fatalf("anchor key reused as release key error=%v", err)
	}

	withQualificationPacketBuild(t, strings.Repeat("5", 40), false)
	oldBuildArguments := append(qualificationLevelFiveArguments(fixture.durabilityPath, fixture.soakPath, fixture.destructivePath, fixture.artifacts, fixture.anchor),
		"--release-signing-key", fixture.releasePrivatePath, "--out", filepath.Join(t.TempDir(), "old-build.json"))
	if err := run(oldBuildArguments, &output, &output); err == nil || !strings.Contains(err.Error(), "clean verifier") {
		t.Fatalf("old packet signer build error=%v", err)
	}
	withQualificationPacketBuild(t, qualificationTestRevision, true)
	dirtyBuildArguments := append(qualificationLevelFiveArguments(fixture.durabilityPath, fixture.soakPath, fixture.destructivePath, fixture.artifacts, fixture.anchor),
		"--release-signing-key", fixture.releasePrivatePath, "--out", filepath.Join(t.TempDir(), "dirty-build.json"))
	if err := run(dirtyBuildArguments, &output, &output); err == nil || !strings.Contains(err.Error(), "clean verifier") {
		t.Fatalf("dirty packet signer build error=%v", err)
	}
}

func TestQualificationReleasePacketCannotSignPartialOrLowerLevelEvidence(t *testing.T) {
	withQualificationPacketBuild(t, qualificationTestRevision, false)
	durability, soak := qualificationFixtures()
	durabilityPath, _ := writeQualificationFixture(t, "durability.json", durability)
	soakPath, _ := writeQualificationFixture(t, "soak.json", soak)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	privatePath := filepath.Join(directory, "release.key")
	if err := writeAnchorQualificationKey(privatePath, privateKey, 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{
		"qualification-check", "--durability-receipt", durabilityPath, "--soak-receipt", soakPath,
		"--source-revision", qualificationTestRevision, "--require-level", "3",
	}
	var output bytes.Buffer
	if err := run(append(append([]string(nil), base...), "--release-signing-key", privatePath), &output, &output); err == nil || !strings.Contains(err.Error(), "both") {
		t.Fatalf("one-sided packet signing arguments error=%v", err)
	}
	lower := append(append([]string(nil), base...), "--release-signing-key", privatePath, "--out", filepath.Join(directory, "lower.json"))
	if err := run(lower, &output, &output); err == nil || !strings.Contains(err.Error(), "require-level 5") {
		t.Fatalf("lower-level packet signing error=%v", err)
	}
}

func TestQualificationPacketKeygenIsPrivateAndExclusive(t *testing.T) {
	directory := t.TempDir()
	privatePath, publicPath := filepath.Join(directory, "release.key"), filepath.Join(directory, "release.pub")
	if err := run([]string{"qualification-packet-keygen", "--private", privatePath, "--public", publicPath}, ioDiscard{}, ioDiscard{}); err != nil {
		t.Fatal(err)
	}
	privateInfo, err := os.Stat(privatePath)
	if err != nil || privateInfo.Mode().Perm() != 0o600 {
		t.Fatalf("private mode=%v err=%v", privateInfo.Mode(), err)
	}
	if err := run([]string{"qualification-packet-keygen", "--private", privatePath, "--public", publicPath}, ioDiscard{}, ioDiscard{}); err == nil {
		t.Fatal("release packet keygen overwrote existing keys")
	}
	secondPrivatePath := filepath.Join(directory, "second-release.key")
	if err := run([]string{"qualification-packet-keygen", "--private", secondPrivatePath, "--public", publicPath}, ioDiscard{}, ioDiscard{}); err == nil {
		t.Fatal("release packet keygen accepted an existing public key")
	}
	if _, err := os.Stat(secondPrivatePath); !os.IsNotExist(err) {
		t.Fatalf("incomplete private key remains after public key failure: %v", err)
	}
}

type qualificationPacketFixture struct {
	durabilityPath     string
	soakPath           string
	destructivePath    string
	artifacts          qualificationArtifactTestFixture
	anchor             qualificationAnchorTestFixture
	releasePrivatePath string
	releasePublicPath  string
	releasePrivateKey  ed25519.PrivateKey
	releasePublicKey   ed25519.PublicKey
}

func newQualificationPacketFixture(t *testing.T) qualificationPacketFixture {
	t.Helper()
	withQualificationDestructiveRecomputeBypass(t)
	durability, soak := qualificationFixtures()
	durabilityPath, durabilityRaw := writeQualificationFixture(t, "durability.json", durability)
	soakPath, soakRaw := writeQualificationFixture(t, "soak.json", soak)
	destructive, artifacts := qualificationDestructiveFixture(t, durability, soak, durabilityRaw, soakRaw)
	destructivePath, _ := writeQualificationFixture(t, "destructive.json", destructive)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	privatePath, publicPath := filepath.Join(directory, "release.key"), filepath.Join(directory, "release.pub")
	if err := writeAnchorQualificationKey(privatePath, privateKey, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeAnchorQualificationKey(publicPath, publicKey, 0o644); err != nil {
		t.Fatal(err)
	}
	return qualificationPacketFixture{
		durabilityPath: durabilityPath, soakPath: soakPath, destructivePath: destructivePath, artifacts: artifacts, anchor: qualificationAnchorFixture(t),
		releasePrivatePath: privatePath, releasePublicPath: publicPath, releasePrivateKey: privateKey, releasePublicKey: publicKey,
	}
}

func qualificationPacketVerifyArguments(packetPath string, fixture qualificationPacketFixture) []string {
	arguments := []string{
		"qualification-packet-verify", "--packet", packetPath, "--release-public-key", fixture.releasePublicPath,
		"--source-revision", qualificationTestRevision, "--durability-receipt", fixture.durabilityPath,
		"--soak-receipt", fixture.soakPath, "--destructive-record", fixture.destructivePath,
		"--environment-record", fixture.artifacts.environmentPath,
		"--artifacts-root", fixture.artifacts.rootPath, "--artifacts-index", fixture.artifacts.indexPath,
		"--anchor-public-key", fixture.anchor.publicKeyPath,
	}
	for _, path := range fixture.anchor.phasePaths {
		arguments = append(arguments, "--anchor-phase-receipt", path)
	}
	return append(arguments, "--anchor-history-receipt", fixture.anchor.historyReceiptPath, "--anchor-history", fixture.anchor.historyPath)
}

func withQualificationPacketBuild(t *testing.T, revision string, modified bool) {
	t.Helper()
	previous := qualificationPacketBuildIdentity
	qualificationPacketBuildIdentity = func() (string, bool) { return revision, modified }
	t.Cleanup(func() { qualificationPacketBuildIdentity = previous })
}
