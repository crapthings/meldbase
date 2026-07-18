package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	qualificationArtifactIndexSchema      uint32 = 1
	qualificationArtifactMaximumEntries          = 10_000
	qualificationArtifactMaximumPathBytes        = 1_024
)

type qualificationArtifactIndex struct {
	SchemaVersion  uint32                       `json:"schemaVersion"`
	SourceRevision string                       `json:"sourceRevision"`
	TotalBytes     uint64                       `json:"totalBytes"`
	Entries        []qualificationArtifactEntry `json:"entries"`
}

type qualificationArtifactEntry struct {
	Path   string `json:"path"`
	Bytes  uint64 `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type verifiedQualificationArtifactIndex struct {
	Raw     []byte
	Root    string
	Index   qualificationArtifactIndex
	Entries map[string]qualificationArtifactEntry
}

func runQualificationArtifactsIndexBuild(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-artifacts-index-build", flag.ContinueOnError)
	flags.SetOutput(stderr)
	rootPath := flags.String("root", "", "quiescent secured artifact directory")
	sourceRevision := flags.String("source-revision", "", "exact 40- or 64-hex release revision")
	outputPath := flags.String("out", "", "new content-addressed artifact index outside the root")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *rootPath == "" || !validDurabilitySourceRevision(*sourceRevision) || *outputPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-artifacts-index-build requires --root, --source-revision and --out")
	}
	root, err := qualificationArtifactRoot(*rootPath)
	if err != nil {
		return err
	}
	output, err := filepath.Abs(filepath.Clean(*outputPath))
	if err != nil {
		return err
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return err
	}
	output = filepath.Join(resolvedParent, filepath.Base(output))
	if qualificationPathWithin(root, output) {
		return errors.New("artifact index output must be outside the indexed root")
	}
	index, err := buildQualificationArtifactIndex(root, *sourceRevision)
	if err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(output, index); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(index)
}

func runQualificationArtifactsIndexVerify(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-artifacts-index-verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	rootPath := flags.String("root", "", "secured artifact directory to rehash")
	indexPath := flags.String("index", "", "schema-1 content-addressed artifact index")
	sourceRevision := flags.String("source-revision", "", "exact 40- or 64-hex release revision")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *rootPath == "" || *indexPath == "" || !validDurabilitySourceRevision(*sourceRevision) || flags.NArg() != 0 {
		return errors.New("qualification-artifacts-index-verify requires --root, --index and --source-revision")
	}
	verified, err := verifyQualificationArtifactIndex(*rootPath, *indexPath, *sourceRevision)
	if err != nil {
		return err
	}
	result := struct {
		SchemaVersion  uint32 `json:"schemaVersion"`
		SourceRevision string `json:"sourceRevision"`
		IndexSHA256    string `json:"indexSha256"`
		Entries        int    `json:"entries"`
		TotalBytes     uint64 `json:"totalBytes"`
		Passed         bool   `json:"passed"`
	}{1, verified.Index.SourceRevision, qualificationSHA256(verified.Raw), len(verified.Index.Entries), verified.Index.TotalBytes, true}
	return json.NewEncoder(stdout).Encode(result)
}

func buildQualificationArtifactIndex(root, revision string) (qualificationArtifactIndex, error) {
	index := qualificationArtifactIndex{SchemaVersion: qualificationArtifactIndexSchema, SourceRevision: revision}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact root contains symbolic link %q", path)
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type() != 0 && !entry.Type().IsRegular() {
			return fmt.Errorf("artifact root contains non-regular file %q", path)
		}
		if len(index.Entries) >= qualificationArtifactMaximumEntries {
			return fmt.Errorf("artifact root exceeds %d files", qualificationArtifactMaximumEntries)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if err := validateQualificationArtifactPath(relative); err != nil {
			return fmt.Errorf("artifact %q: %w", path, err)
		}
		size, digest, err := hashQualificationArtifact(path)
		if err != nil {
			return fmt.Errorf("artifact %q: %w", path, err)
		}
		if ^uint64(0)-index.TotalBytes < size {
			return errors.New("artifact byte total overflows uint64")
		}
		index.TotalBytes += size
		index.Entries = append(index.Entries, qualificationArtifactEntry{Path: relative, Bytes: size, SHA256: digest})
		return nil
	})
	if err != nil {
		return qualificationArtifactIndex{}, err
	}
	if len(index.Entries) == 0 {
		return qualificationArtifactIndex{}, errors.New("artifact root must contain at least one regular file")
	}
	sort.Slice(index.Entries, func(i, j int) bool { return index.Entries[i].Path < index.Entries[j].Path })
	return index, nil
}

func verifyQualificationArtifactIndex(rootPath, indexPath, revision string) (verifiedQualificationArtifactIndex, error) {
	root, err := qualificationArtifactRoot(rootPath)
	if err != nil {
		return verifiedQualificationArtifactIndex{}, err
	}
	var expected qualificationArtifactIndex
	raw, err := readQualificationReceipt(indexPath, &expected)
	if err != nil {
		return verifiedQualificationArtifactIndex{}, fmt.Errorf("artifact index: %w", err)
	}
	if err := validateQualificationArtifactIndex(expected, revision); err != nil {
		return verifiedQualificationArtifactIndex{}, err
	}
	actual, err := buildQualificationArtifactIndex(root, revision)
	if err != nil {
		return verifiedQualificationArtifactIndex{}, err
	}
	if len(actual.Entries) != len(expected.Entries) || actual.TotalBytes != expected.TotalBytes {
		return verifiedQualificationArtifactIndex{}, errors.New("artifact root file set or total bytes differs from index")
	}
	entries := make(map[string]qualificationArtifactEntry, len(expected.Entries))
	for index := range expected.Entries {
		want, got := expected.Entries[index], actual.Entries[index]
		if want != got {
			return verifiedQualificationArtifactIndex{}, fmt.Errorf("artifact %q differs from index", want.Path)
		}
		entries[want.Path] = want
	}
	return verifiedQualificationArtifactIndex{Raw: raw, Root: root, Index: expected, Entries: entries}, nil
}

func validateQualificationArtifactIndex(index qualificationArtifactIndex, revision string) error {
	if index.SchemaVersion != qualificationArtifactIndexSchema || index.SourceRevision != revision ||
		len(index.Entries) == 0 || len(index.Entries) > qualificationArtifactMaximumEntries {
		return errors.New("artifact index schema, revision or entry count is invalid")
	}
	var total uint64
	previous := ""
	for ordinal, entry := range index.Entries {
		if err := validateQualificationArtifactPath(entry.Path); err != nil {
			return fmt.Errorf("artifact index entry %d: %w", ordinal+1, err)
		}
		if entry.Path <= previous || !qualificationHexDigest(entry.SHA256) {
			return fmt.Errorf("artifact index entry %d is unsorted, repeated or malformed", ordinal+1)
		}
		if ^uint64(0)-total < entry.Bytes {
			return errors.New("artifact index byte total overflows uint64")
		}
		total += entry.Bytes
		previous = entry.Path
	}
	if total != index.TotalBytes {
		return errors.New("artifact index total bytes does not match its entries")
	}
	return nil
}

func qualificationArtifactRoot(path string) (string, error) {
	root, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("artifact root must be a real directory, not a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func validateQualificationArtifactPath(path string) error {
	if path == "" || len(path) > qualificationArtifactMaximumPathBytes || !utf8.ValidString(path) ||
		strings.Contains(path, "\\") || filepath.IsAbs(path) || filepath.ToSlash(filepath.Clean(path)) != path ||
		path == "." || path == ".." || strings.HasPrefix(path, "../") {
		return errors.New("path must be a bounded canonical relative UTF-8 path")
	}
	for _, character := range path {
		if character < 0x20 || character == 0x7f {
			return errors.New("path contains a control character")
		}
	}
	return nil
}

func hashQualificationArtifact(path string) (uint64, string, error) {
	pathBefore, err := os.Lstat(path)
	if err != nil || pathBefore.Mode()&os.ModeSymlink != 0 || !pathBefore.Mode().IsRegular() {
		return 0, "", errors.New("artifact path must name a regular file, not a symbolic link")
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() < 0 || !os.SameFile(pathBefore, before) {
		return 0, "", errors.New("artifact must be a regular file")
	}
	hash := sha256.New()
	written, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", err
	}
	after, err := file.Stat()
	pathAfter, pathErr := os.Lstat(path)
	if err != nil || !after.Mode().IsRegular() || written != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return 0, "", errors.New("artifact changed while hashing")
	}
	if pathErr != nil || pathAfter.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, pathAfter) {
		return 0, "", errors.New("artifact path was replaced while hashing")
	}
	return uint64(written), hex.EncodeToString(hash.Sum(nil)), nil
}

func qualificationPathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !filepath.IsAbs(relative) && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func requireQualificationArtifactPaths(index verifiedQualificationArtifactIndex, paths ...string) error {
	for _, candidate := range paths {
		absolute, err := qualificationArtifactCandidate(candidate)
		if err != nil || !qualificationPathWithin(index.Root, absolute) || absolute == index.Root {
			return fmt.Errorf("required artifact %q is outside the secured artifact root", candidate)
		}
		relative, err := filepath.Rel(index.Root, absolute)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if _, exists := index.Entries[relative]; !exists {
			return fmt.Errorf("required artifact %q is absent from the secured artifact index", candidate)
		}
	}
	return nil
}

func qualificationArtifactEntryForPath(index verifiedQualificationArtifactIndex, candidate string) (qualificationArtifactEntry, error) {
	absolute, err := qualificationArtifactCandidate(candidate)
	if err != nil || !qualificationPathWithin(index.Root, absolute) || absolute == index.Root {
		return qualificationArtifactEntry{}, fmt.Errorf("required artifact %q is outside the secured artifact root", candidate)
	}
	relative, err := filepath.Rel(index.Root, absolute)
	if err != nil {
		return qualificationArtifactEntry{}, err
	}
	entry, exists := index.Entries[filepath.ToSlash(relative)]
	if !exists {
		return qualificationArtifactEntry{}, fmt.Errorf("required artifact %q is absent from the secured artifact index", candidate)
	}
	return entry, nil
}

func qualificationArtifactEntryForDigest(index verifiedQualificationArtifactIndex, digest string, size uint64) (qualificationArtifactEntry, error) {
	var matched qualificationArtifactEntry
	for _, entry := range index.Entries {
		if entry.SHA256 != digest || entry.Bytes != size {
			continue
		}
		if matched.Path != "" {
			return qualificationArtifactEntry{}, errors.New("secured artifact index repeats a required content digest")
		}
		matched = entry
	}
	if matched.Path == "" {
		return qualificationArtifactEntry{}, errors.New("required content digest is absent from the secured artifact index")
	}
	return matched, nil
}

func qualificationArtifactPathForDigest(index verifiedQualificationArtifactIndex, digest string) (string, error) {
	if !qualificationHexDigest(digest) {
		return "", errors.New("required content digest is malformed")
	}
	var matched qualificationArtifactEntry
	for _, entry := range index.Entries {
		if entry.SHA256 != digest {
			continue
		}
		if matched.Path != "" {
			return "", errors.New("secured artifact index repeats a required content digest")
		}
		matched = entry
	}
	if matched.Path == "" {
		return "", errors.New("required content digest is absent from the secured artifact index")
	}
	return filepath.Join(index.Root, filepath.FromSlash(matched.Path)), nil
}

func qualificationArtifactPathForReference(index verifiedQualificationArtifactIndex, originalPath, digest string) (string, error) {
	if !filepath.IsAbs(originalPath) || !qualificationHexDigest(digest) {
		return "", errors.New("artifact reference path or digest is malformed")
	}
	original := filepath.ToSlash(filepath.Clean(originalPath))
	var candidates []qualificationArtifactEntry
	for _, entry := range index.Entries {
		if entry.SHA256 == digest {
			candidates = append(candidates, entry)
		}
	}
	if len(candidates) == 0 {
		return "", errors.New("referenced artifact digest is absent from the secured artifact index")
	}
	best := ""
	for _, candidate := range candidates {
		if original != candidate.Path && !strings.HasSuffix(original, "/"+candidate.Path) {
			continue
		}
		if len(candidate.Path) > len(best) {
			best = candidate.Path
		} else if len(candidate.Path) == len(best) {
			return "", errors.New("referenced artifact digest and path suffix are ambiguous")
		}
	}
	if best == "" {
		if len(candidates) == 1 {
			return "", errors.New("referenced artifact path suffix differs from the secured artifact index")
		}
		return "", errors.New("repeated artifact digest cannot be resolved by the original path suffix")
	}
	return filepath.Join(index.Root, filepath.FromSlash(best)), nil
}

func qualificationArtifactCandidate(candidate string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(candidate))
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("required artifact must be a regular file, not a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}
