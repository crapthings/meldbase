package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQualificationArtifactIndexBuildAndVerifyExactTree(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		"alpha.json":         "{\"evidence\":1}\n",
		"nested/database.db": "database artifact bytes",
	} {
		if err := os.WriteFile(filepath.Join(root, path), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	indexPath := filepath.Join(t.TempDir(), "artifact-index.json")
	var output bytes.Buffer
	if err := run([]string{
		"qualification-artifacts-index-build", "--root", root, "--source-revision", qualificationTestRevision, "--out", indexPath,
	}, &output, &output); err != nil {
		t.Fatalf("build index=%v output=%s", err, output.String())
	}
	var index qualificationArtifactIndex
	if err := json.Unmarshal(output.Bytes(), &index); err != nil {
		t.Fatal(err)
	}
	if index.SchemaVersion != qualificationArtifactIndexSchema || index.SourceRevision != qualificationTestRevision ||
		len(index.Entries) != 2 || index.Entries[0].Path != "alpha.json" || index.Entries[1].Path != "nested/database.db" {
		t.Fatalf("index=%+v", index)
	}
	output.Reset()
	if err := run([]string{
		"qualification-artifacts-index-verify", "--root", root, "--index", indexPath, "--source-revision", qualificationTestRevision,
	}, &output, &output); err != nil || !strings.Contains(output.String(), `"passed":true`) {
		t.Fatalf("verify index=%v output=%s", err, output.String())
	}
	if err := run([]string{
		"qualification-artifacts-index-build", "--root", root, "--source-revision", qualificationTestRevision, "--out", indexPath,
	}, &output, &output); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Fatalf("index output was overwritten: %v", err)
	}
	inside := filepath.Join(root, "index.json")
	if err := run([]string{
		"qualification-artifacts-index-build", "--root", root, "--source-revision", qualificationTestRevision, "--out", inside,
	}, &output, &output); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("self-index accepted: %v", err)
	}
}

func TestQualificationArtifactIndexRejectsMutationMissingExtraAndWrongRevision(t *testing.T) {
	root := t.TempDir()
	artifact := filepath.Join(root, "artifact.bin")
	original := []byte("original artifact")
	if err := os.WriteFile(artifact, original, 0o600); err != nil {
		t.Fatal(err)
	}
	index, err := buildQualificationArtifactIndex(root, qualificationTestRevision)
	if err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(t.TempDir(), "index.json")
	if err := writeJSONExclusiveDurable(indexPath, index); err != nil {
		t.Fatal(err)
	}
	verify := func() error {
		_, err := verifyQualificationArtifactIndex(root, indexPath, qualificationTestRevision)
		return err
	}
	if err := os.WriteFile(artifact, []byte("tampered artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("tampered artifact error=%v", err)
	}
	if err := os.WriteFile(artifact, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "unindexed.bin"), []byte("extra"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err == nil || !strings.Contains(err.Error(), "file set") {
		t.Fatalf("extra artifact error=%v", err)
	}
	if err := os.Remove(filepath.Join(root, "unindexed.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(artifact); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("missing artifact error=%v", err)
	}
	if _, err := verifyQualificationArtifactIndex(root, indexPath, strings.Repeat("f", 40)); err == nil || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("wrong revision error=%v", err)
	}
}

func TestQualificationArtifactIndexRejectsLinksSpecialAndForgedPaths(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := buildQualificationArtifactIndex(root, qualificationTestRevision); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("symbolic link error=%v", err)
	}
	forged := qualificationArtifactIndex{
		SchemaVersion: qualificationArtifactIndexSchema, SourceRevision: qualificationTestRevision,
		Entries: []qualificationArtifactEntry{{Path: "../escape", SHA256: strings.Repeat("a", 64)}},
	}
	if err := validateQualificationArtifactIndex(forged, qualificationTestRevision); err == nil || !strings.Contains(err.Error(), "canonical relative") {
		t.Fatalf("forged traversal error=%v", err)
	}
	forged.Entries[0].Path = "duplicate"
	forged.Entries = append(forged.Entries, forged.Entries[0])
	if err := validateQualificationArtifactIndex(forged, qualificationTestRevision); err == nil || !strings.Contains(err.Error(), "unsorted") {
		t.Fatalf("duplicate entry error=%v", err)
	}
}

func TestQualificationArtifactDigestLookupRequiresUniqueContent(t *testing.T) {
	root := t.TempDir()
	content := []byte("same receipt bytes\n")
	for _, name := range []string{"receipt-a.json", "receipt-b.json"} {
		if err := os.WriteFile(filepath.Join(root, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	index, err := buildQualificationArtifactIndex(root, qualificationTestRevision)
	if err != nil {
		t.Fatal(err)
	}
	entries := make(map[string]qualificationArtifactEntry, len(index.Entries))
	for _, entry := range index.Entries {
		entries[entry.Path] = entry
	}
	verified := verifiedQualificationArtifactIndex{Root: root, Index: index, Entries: entries}
	if _, err := qualificationArtifactPathForDigest(verified, qualificationSHA256(content)); err == nil || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("duplicate digest error=%v", err)
	}
	resolved, err := qualificationArtifactPathForReference(verified, "/old/archive/receipt-a.json", qualificationSHA256(content))
	if err != nil || resolved != filepath.Join(root, "receipt-a.json") {
		t.Fatalf("suffix-resolved path=%q error=%v", resolved, err)
	}
	if _, err := qualificationArtifactPathForReference(verified, "/old/archive/receipt.json", qualificationSHA256(content)); err == nil || !strings.Contains(err.Error(), "cannot be resolved") {
		t.Fatalf("ambiguous reference error=%v", err)
	}
	if _, err := qualificationArtifactPathForDigest(verified, strings.Repeat("f", 64)); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("missing digest error=%v", err)
	}
	uniqueRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(uniqueRoot, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(uniqueRoot, "nested", "artifact.bin"), []byte("unique"), 0o600); err != nil {
		t.Fatal(err)
	}
	uniqueIndex, err := buildQualificationArtifactIndex(uniqueRoot, qualificationTestRevision)
	if err != nil {
		t.Fatal(err)
	}
	uniqueEntries := map[string]qualificationArtifactEntry{uniqueIndex.Entries[0].Path: uniqueIndex.Entries[0]}
	uniqueVerified := verifiedQualificationArtifactIndex{Root: uniqueRoot, Index: uniqueIndex, Entries: uniqueEntries}
	if _, err := qualificationArtifactPathForReference(uniqueVerified, "/old/root/renamed.bin", qualificationSHA256([]byte("unique"))); err == nil || !strings.Contains(err.Error(), "path suffix differs") {
		t.Fatalf("renamed unique artifact error=%v", err)
	}
}
