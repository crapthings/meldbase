package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDestructiveCorruptionCampaignReproducesAndBindsSource(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "source.meld")
	if err := seedDestructiveCapacityDatabase(database, []byte("stable")); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(directory, "corruption-receipt.json")
	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"destructive-corruption-check", "--database", database, "--out", receiptPath, "--page-samples", "4",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("campaign=%v stderr=%s", err, stderr.String())
	}
	var receipt destructiveCorruptionReceipt
	if err := json.Unmarshal(stdout.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Passed || receipt.MutationCount != 4*len(destructiveCorruptionOffsets) ||
		receipt.MutationCount != receipt.DetectedCount+receipt.ValidOutcomeCount || len(receipt.SampledPages) != 4 {
		t.Fatalf("receipt=%+v", receipt)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run([]string{"destructive-corruption-receipt-check", "--receipt", receiptPath}, &stdout, &stderr); err != nil {
		t.Fatalf("receipt check=%v stderr=%s", err, stderr.String())
	}
	var checked struct {
		MutationCount int  `json:"mutationCount"`
		Passed        bool `json:"passed"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &checked); err != nil || !checked.Passed || checked.MutationCount != receipt.MutationCount {
		t.Fatalf("checked=%+v err=%v", checked, err)
	}

	raw, err := os.ReadFile(database)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(database, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run([]string{"destructive-corruption-receipt-check", "--receipt", receiptPath}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("mutated source error=%v output=%s", err, stderr.String())
	}
}

func TestDeterministicCorruptionPageSamplesCoverEndpoints(t *testing.T) {
	if pages := deterministicPageSamples(4, 8); !equalUint64s(pages, []uint64{0, 1, 2, 3}) {
		t.Fatalf("all pages=%v", pages)
	}
	pages := deterministicPageSamples(1_000, 4)
	if !equalUint64s(pages, []uint64{0, 333, 666, 999}) {
		t.Fatalf("spread pages=%v", pages)
	}
}
