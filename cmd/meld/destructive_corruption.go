package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/crapthings/meldbase"
	storagev2 "github.com/crapthings/meldbase/internal/storage/v2"
)

const (
	destructiveCorruptionReceiptSchema uint32 = 1
	maxCorruptionPageSamples                  = 128
)

var destructiveCorruptionOffsets = []uint64{0, 7, 8, 31, 63, 127, 223, 224, 255, 511, 1023, 2047, 3071, storagev2.PageSize - 1}

type destructiveCorruptionReceipt struct {
	SchemaVersion     uint32                        `json:"schemaVersion"`
	SourceRevision    string                        `json:"sourceRevision,omitempty"`
	BuildRevision     string                        `json:"buildRevision,omitempty"`
	BuildModified     bool                          `json:"buildModified"`
	GOOS              string                        `json:"goos"`
	GOARCH            string                        `json:"goarch"`
	GoVersion         string                        `json:"goVersion"`
	DatabaseArtifact  string                        `json:"databaseArtifact"`
	DatabaseSHA256    string                        `json:"databaseSha256"`
	Baseline          meldbase.V2VerificationReport `json:"baseline"`
	SampledPages      []uint64                      `json:"sampledPages"`
	OffsetsWithinPage []uint64                      `json:"offsetsWithinPage"`
	MutationCount     int                           `json:"mutationCount"`
	DetectedCount     int                           `json:"detectedCount"`
	ValidOutcomeCount int                           `json:"validOutcomeCount"`
	ValidOutcomeBySeq map[string]int                `json:"validOutcomeBySequence"`
	StartedAt         time.Time                     `json:"startedAt"`
	FinishedAt        time.Time                     `json:"finishedAt"`
	Passed            bool                          `json:"passed"`
}

type destructiveCorruptionCampaignResult struct {
	Baseline          meldbase.V2VerificationReport
	SampledPages      []uint64
	MutationCount     int
	DetectedCount     int
	ValidOutcomeCount int
	ValidOutcomeBySeq map[string]int
}

func runDestructiveCorruptionCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-corruption-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("database", "", "existing V2 database that will never be modified")
	outputPath := flags.String("out", "", "new machine-readable corruption campaign receipt")
	pageSamples := flags.Int("page-samples", maxCorruptionPageSamples, "maximum deterministically sampled physical pages")
	sourceRevision := flags.String("source-revision", "", "optional 40- or 64-hex source revision")
	requireClean := flags.Bool("require-clean-source", false, "require a clean binary matching --source-revision")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *databasePath == "" || *outputPath == "" || *pageSamples < 2 || *pageSamples > maxCorruptionPageSamples {
		return fmt.Errorf("destructive-corruption-check requires --database, --out and --page-samples between 2 and %d", maxCorruptionPageSamples)
	}
	if *sourceRevision != "" && !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("destructive-corruption-check --source-revision must be 40 or 64 hexadecimal characters")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (*sourceRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("destructive-corruption-check clean source verification failed")
	}
	database, err := existingRegularAbsolutePath(*databasePath)
	if err != nil {
		return fmt.Errorf("destructive-corruption-check database: %w", err)
	}
	output, err := newAbsolutePath(*outputPath)
	if err != nil {
		return fmt.Errorf("destructive-corruption-check output: %w", err)
	}
	started := time.Now().UTC()
	result, err := executeDestructiveCorruptionCampaign(context.Background(), database, *pageSamples)
	if err != nil {
		return err
	}
	databaseHash, err := hashRegularFile(database, 1<<30)
	if err != nil || databaseHash != result.Baseline.SHA256 {
		return errors.New("destructive-corruption-check source database changed during the campaign")
	}
	receipt := destructiveCorruptionReceipt{
		SchemaVersion: destructiveCorruptionReceiptSchema, SourceRevision: *sourceRevision,
		BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		DatabaseArtifact: database, DatabaseSHA256: databaseHash, Baseline: result.Baseline,
		SampledPages: result.SampledPages, OffsetsWithinPage: append([]uint64(nil), destructiveCorruptionOffsets...),
		MutationCount: result.MutationCount, DetectedCount: result.DetectedCount,
		ValidOutcomeCount: result.ValidOutcomeCount, ValidOutcomeBySeq: result.ValidOutcomeBySeq,
		StartedAt: started, FinishedAt: time.Now().UTC(), Passed: true,
	}
	if err := validateDestructiveCorruptionReceipt(receipt); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(output, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

func runDestructiveCorruptionReceiptCheck(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("destructive-corruption-receipt-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	receiptPath := flags.String("receipt", "", "corruption receipt whose source database will be rechecked")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *receiptPath == "" {
		return errors.New("destructive-corruption-receipt-check requires --receipt")
	}
	var receipt destructiveCorruptionReceipt
	raw, err := readQualificationReceipt(*receiptPath, &receipt)
	if err != nil {
		return err
	}
	if err := validateDestructiveCorruptionReceipt(receipt); err != nil {
		return err
	}
	if err := recheckDestructiveCorruptionReceipt(receipt); err != nil {
		return err
	}
	resultPacket := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		ReceiptSHA256 string `json:"receiptSha256"`
		MutationCount int    `json:"mutationCount"`
		Passed        bool   `json:"passed"`
	}{SchemaVersion: 1, ReceiptSHA256: qualificationSHA256(raw), MutationCount: receipt.MutationCount, Passed: true}
	return json.NewEncoder(stdout).Encode(resultPacket)
}

func recheckDestructiveCorruptionReceipt(receipt destructiveCorruptionReceipt) error {
	actualHash, err := hashRegularFile(receipt.DatabaseArtifact, 1<<30)
	if err != nil || actualHash != receipt.DatabaseSHA256 {
		return errors.New("corruption receipt database artifact is missing or mismatched")
	}
	result, err := executeDestructiveCorruptionCampaign(context.Background(), receipt.DatabaseArtifact, len(receipt.SampledPages))
	if err != nil {
		return err
	}
	if result.Baseline != receipt.Baseline || !equalUint64s(result.SampledPages, receipt.SampledPages) ||
		result.MutationCount != receipt.MutationCount || result.DetectedCount != receipt.DetectedCount ||
		result.ValidOutcomeCount != receipt.ValidOutcomeCount || !equalStringIntMaps(result.ValidOutcomeBySeq, receipt.ValidOutcomeBySeq) {
		return errors.New("corruption receipt does not reproduce its campaign")
	}
	return nil
}

func executeDestructiveCorruptionCampaign(ctx context.Context, database string, pageSampleLimit int) (result destructiveCorruptionCampaignResult, resultErr error) {
	if pageSampleLimit < 2 || pageSampleLimit > maxCorruptionPageSamples {
		return result, errors.New("corruption campaign page sample limit is invalid")
	}
	baseline, err := meldbase.VerifyV2File(ctx, database)
	if err != nil || !baseline.Verified || !baseline.IndexContentsVerified || !baseline.IndexBuildContentsVerified ||
		baseline.ValidMetaSlots != 2 || baseline.TrailingBytes != 0 || baseline.PhysicalPages < 2 {
		return result, fmt.Errorf("corruption campaign requires a completely verified V2 source: %w", err)
	}
	allowedSequences, databaseID, err := corruptionAllowedMetaOutcomes(database)
	if err != nil {
		return result, err
	}
	pages := deterministicPageSamples(baseline.PhysicalPages, pageSampleLimit)
	temporary, err := os.CreateTemp("", ".meldbase-corruption-*.meld")
	if err != nil {
		return result, err
	}
	temporaryPath := temporary.Name()
	defer func() {
		resultErr = errors.Join(resultErr, temporary.Close(), os.Remove(temporaryPath))
	}()
	source, err := os.Open(database)
	if err != nil {
		return result, err
	}
	if _, err := io.Copy(temporary, source); err != nil {
		_ = source.Close()
		return result, err
	}
	if err := source.Close(); err != nil {
		return result, err
	}
	if err := temporary.Sync(); err != nil {
		return result, err
	}
	result = destructiveCorruptionCampaignResult{Baseline: baseline, SampledPages: pages, ValidOutcomeBySeq: make(map[string]int)}
	for _, page := range pages {
		for _, within := range destructiveCorruptionOffsets {
			offset := int64(page*storagev2.PageSize + within)
			original := []byte{0}
			if _, err := temporary.ReadAt(original, offset); err != nil {
				return result, err
			}
			mutated := []byte{original[0] ^ 1}
			if _, err := temporary.WriteAt(mutated, offset); err != nil {
				return result, err
			}
			if err := temporary.Sync(); err != nil {
				return result, err
			}
			verification, verifyErr := meldbase.VerifyV2File(ctx, temporaryPath)
			if _, err := temporary.WriteAt(original, offset); err != nil {
				return result, err
			}
			if err := temporary.Sync(); err != nil {
				return result, err
			}
			result.MutationCount++
			if verifyErr != nil {
				if !errors.Is(verifyErr, meldbase.ErrCorrupt) && !errors.Is(verifyErr, meldbase.ErrUnsupportedFormat) &&
					!errors.Is(verifyErr, meldbase.ErrVerificationUnsupported) {
					return result, fmt.Errorf("corruption mutation page %d offset %d produced an infrastructure error: %w", page, within, verifyErr)
				}
				result.DetectedCount++
				continue
			}
			if !verification.Verified || !verification.IndexContentsVerified || !verification.IndexBuildContentsVerified ||
				verification.DatabaseIDHex != databaseID || verification.SHA256 == baseline.SHA256 {
				return result, fmt.Errorf("corruption mutation page %d offset %d produced an invalid successful verification", page, within)
			}
			if _, allowed := allowedSequences[verification.CommitSequence]; !allowed {
				return result, fmt.Errorf("corruption mutation page %d offset %d exposed unexpected sequence %d", page, within, verification.CommitSequence)
			}
			result.ValidOutcomeCount++
			result.ValidOutcomeBySeq[strconv.FormatUint(verification.CommitSequence, 10)]++
		}
	}
	return result, nil
}

func corruptionAllowedMetaOutcomes(path string) (map[uint64]struct{}, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	allowed := make(map[uint64]struct{}, 2)
	databaseID := ""
	for slot := range 2 {
		page := make([]byte, storagev2.PageSize)
		if _, err := file.ReadAt(page, int64(slot*storagev2.PageSize)); err != nil {
			return nil, "", err
		}
		meta, err := storagev2.DecodeMeta(page)
		if err != nil {
			continue
		}
		id := fmt.Sprintf("%x", meta.DatabaseID)
		if databaseID != "" && databaseID != id {
			return nil, "", errors.New("corruption campaign source has mismatched database identities")
		}
		databaseID = id
		allowed[meta.CommitSequence] = struct{}{}
	}
	if databaseID == "" || len(allowed) == 0 {
		return nil, "", errors.New("corruption campaign source has no valid Meta outcome")
	}
	return allowed, databaseID, nil
}

func deterministicPageSamples(physicalPages uint64, limit int) []uint64 {
	if physicalPages <= uint64(limit) {
		pages := make([]uint64, physicalPages)
		for index := range pages {
			pages[index] = uint64(index)
		}
		return pages
	}
	pages := make([]uint64, limit)
	for index := range pages {
		pages[index] = uint64(index) * (physicalPages - 1) / uint64(limit-1)
	}
	return pages
}

func validateDestructiveCorruptionReceipt(receipt destructiveCorruptionReceipt) error {
	if receipt.SchemaVersion != destructiveCorruptionReceiptSchema || receipt.GOOS == "" || receipt.GOARCH == "" || receipt.GoVersion == "" {
		return errors.New("corruption receipt build identity is invalid")
	}
	if (receipt.SourceRevision != "" && !validDurabilitySourceRevision(receipt.SourceRevision)) ||
		(receipt.BuildRevision != "" && !validDurabilitySourceRevision(receipt.BuildRevision)) ||
		!qualificationSafeName(receipt.GOOS, 32) || !qualificationSafeName(receipt.GOARCH, 32) {
		return errors.New("corruption receipt source or runtime identity is invalid")
	}
	if !filepath.IsAbs(receipt.DatabaseArtifact) || !qualificationHexDigest(receipt.DatabaseSHA256) || receipt.DatabaseSHA256 != receipt.Baseline.SHA256 {
		return errors.New("corruption receipt database identity is invalid")
	}
	if !receipt.Baseline.Verified || !receipt.Baseline.IndexContentsVerified || !receipt.Baseline.IndexBuildContentsVerified ||
		receipt.Baseline.ValidMetaSlots != 2 || receipt.Baseline.TrailingBytes != 0 || receipt.Baseline.PhysicalPages < 2 {
		return errors.New("corruption receipt baseline verification is invalid")
	}
	if len(receipt.SampledPages) < 2 || len(receipt.SampledPages) > maxCorruptionPageSamples || !equalUint64s(receipt.OffsetsWithinPage, destructiveCorruptionOffsets) {
		return errors.New("corruption receipt sampling coverage is invalid")
	}
	if receipt.MutationCount != len(receipt.SampledPages)*len(receipt.OffsetsWithinPage) || receipt.MutationCount != receipt.DetectedCount+receipt.ValidOutcomeCount {
		return errors.New("corruption receipt mutation accounting is invalid")
	}
	if receipt.StartedAt.IsZero() || !receipt.FinishedAt.After(receipt.StartedAt) || !receipt.Passed {
		return errors.New("corruption receipt timing or status is invalid")
	}
	if !equalUint64s(receipt.SampledPages, deterministicPageSamples(receipt.Baseline.PhysicalPages, len(receipt.SampledPages))) {
		return errors.New("corruption receipt page selection is not deterministic")
	}
	total := 0
	for sequence, count := range receipt.ValidOutcomeBySeq {
		if _, err := strconv.ParseUint(sequence, 10, 64); err != nil || count <= 0 {
			return errors.New("corruption receipt contains an invalid successful outcome")
		}
		total += count
	}
	if total != receipt.ValidOutcomeCount {
		return errors.New("corruption receipt successful outcome accounting is invalid")
	}
	return nil
}

func existingRegularAbsolutePath(path string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("path must be an existing regular file")
	}
	return absolute, nil
}

func newAbsolutePath(path string) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(absolute); err == nil {
		return "", errors.New("path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return absolute, nil
}

func equalUint64s(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStringIntMaps(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
