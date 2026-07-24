package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/crapthings/meldbase"
)

func TestObserveRepresentativeQuerySignals(t *testing.T) {
	for _, backend := range []string{"memory", "durable"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			report, err := observe(ctx, observerConfig{
				Backend: backend, Documents: 256, Iterations: 1, Warmup: 0,
				PayloadBytes: 8, Scenarios: "all",
			})
			if err != nil {
				t.Fatal(err)
			}
			if report.SchemaVersion != reportSchemaVersion || report.Config.Backend != backend || report.Config.DatabaseRetained {
				t.Fatalf("report metadata=%+v", report)
			}
			if report.Config.Limits.MaxQueryKeysExamined != meldbase.DefaultMaxQueryKeysExamined {
				t.Fatalf("normalized limits=%+v", report.Config.Limits)
			}
			if report.MeasuredTotals.Total != 10 || report.MeasuredTotals.Failed != 0 ||
				report.MeasuredTotals.CollectionScans != 1 || report.MeasuredTotals.IndexScans != 9 ||
				report.MeasuredTotals.PredicateSteps == 0 {
				t.Fatalf("measured totals=%+v", report.MeasuredTotals)
			}

			observations := observationsByName(report)
			indexed := observations["indexed-limit"]
			if indexed.Explain.Stage != "IXSCAN" || indexed.Explain.PlanReason != "secondary_index" ||
				!indexed.Explain.EarlyStopEligible || !indexed.Explain.EarlyStopped ||
				indexed.Measured.EarlyStops != 1 {
				t.Fatalf("indexed-limit=%+v", indexed)
			}

			overlap := observations["overlapping-or"]
			if overlap.Explain.PlanReason != "multi_index_union" || len(overlap.Explain.Sources) != 2 ||
				overlap.Explain.DuplicateCandidateIDs == 0 ||
				overlap.Explain.CandidateIDs <= overlap.Explain.UniqueCandidateIDs ||
				overlap.Ratios.DuplicateCandidatePercent != 25 ||
				!hasAdvice(overlap.Explain.Advice, "high_union_overlap") {
				t.Fatalf("overlapping-or=%+v", overlap)
			}

			rangeScan := observations["range-limit"]
			if rangeScan.Explain.Stage != "IXSCAN" || rangeScan.Explain.EarlyStopEligible ||
				rangeScan.Explain.EarlyStopReason != "range_scan" ||
				!hasAdvice(rangeScan.Explain.Advice, "limit_requires_full_scan") {
				t.Fatalf("range-limit=%+v", rangeScan)
			}

			sorted := observations["sort-pressure"]
			if !sorted.Explain.SortRequired || sorted.Explain.SortBytes == 0 ||
				sorted.Explain.CandidatesRetained == 0 ||
				!hasAdvice(sorted.Explain.Advice, "consider_sort_index") {
				t.Fatalf("sort-pressure=%+v", sorted)
			}

			collectionScan := observations["collection-scan"]
			if collectionScan.Explain.Stage != "COLLSCAN" ||
				collectionScan.Explain.DocumentsExamined != 256 ||
				!hasAdvice(collectionScan.Explain.Advice, "consider_filter_index") ||
				len(collectionScan.Explain.UnindexedPaths) != 1 ||
				collectionScan.Explain.UnindexedPaths[0] != "group" {
				t.Fatalf("collection-scan=%+v", collectionScan)
			}

			separate := observations["and-separate-indexes"]
			if separate.Explain.Stage != "IXSCAN" || separate.Explain.IndexName != "by_status" ||
				separate.Explain.DocumentsExamined != 32 || separate.Explain.CandidatesRetained != 4 ||
				separate.Ratios.DocumentsPerRetained != 8 || !separate.Explain.CompoundIndexOpportunity ||
				!reflect.DeepEqual(separate.Explain.IndexableConjunctPaths, []string{"status", "workspaceId"}) ||
				!hasAdvice(separate.Explain.Advice, "consider_compound_index") {
				t.Fatalf("and-separate-indexes=%+v", separate)
			}

			compound := observations["and-compound-index"]
			if compound.Explain.Stage != "IXSCAN" || compound.Explain.IndexName != "by_workspace_compound_status" ||
				compound.Explain.DocumentsExamined != 4 || compound.Explain.CandidatesRetained != 4 ||
				compound.Ratios.DocumentsPerRetained != 1 || compound.Explain.CompoundIndexOpportunity ||
				!reflect.DeepEqual(compound.Explain.IndexableConjunctPaths, []string{"compoundStatus", "workspaceId"}) ||
				hasAdvice(compound.Explain.Advice, "consider_compound_index") {
				t.Fatalf("and-compound-index=%+v", compound)
			}

			all := observations["array-all-miss"]
			if all.Family != "array-all" || all.Explain.Stage != "IXSCAN" || !all.Explain.ResidualPredicate ||
				all.ReturnedTotal != 0 || all.Explain.Budget.PredicateStepsUsed <= uint64(all.Explain.DocumentsExamined) ||
				all.Ratios.PredicateStepsPerDocument <= 16 {
				t.Fatalf("array-all-miss=%+v", all)
			}
			scalar := observations["array-elem-scalar"]
			object := observations["array-elem-object"]
			for _, arrayObservation := range []scenarioObservation{scalar, object} {
				if arrayObservation.Family != "array-elem-match" || arrayObservation.Explain.Stage != "IXSCAN" ||
					!arrayObservation.Explain.ResidualPredicate || arrayObservation.ReturnedTotal == 0 ||
					arrayObservation.Explain.Budget.PredicateStepsUsed <= uint64(arrayObservation.Explain.DocumentsExamined) ||
					arrayObservation.Ratios.PredicateStepsPerDocument <= 16 {
					t.Fatalf("array scenario=%+v", arrayObservation)
				}
			}
		})
	}
}

func TestObserveRetainsBudgetRejectionEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := observe(ctx, observerConfig{
		Backend: "memory", Documents: 256, Iterations: 2, Warmup: 0,
		PayloadBytes: 8, Scenarios: "overlapping-or",
		Limits: meldbase.ResourceLimits{MaxQueryKeysExamined: 64},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Scenarios) != 1 {
		t.Fatalf("scenarios=%d", len(report.Scenarios))
	}
	observation := report.Scenarios[0]
	if observation.ExplainErrorCode != "query_budget" || observation.Explain.Budget.Exceeded != "keys" ||
		observation.Explain.KeysExamined != 64 {
		t.Fatalf("explain rejection=%+v", observation)
	}
	if observation.RunsAttempted != 2 || observation.RunsFailed != 2 ||
		observation.ExecutionErrorCode != "query_budget" ||
		observation.Measured.Total != 2 || observation.Measured.Failed != 2 ||
		observation.Measured.BudgetRejections != 2 ||
		observation.Measured.BudgetPressureEvents != 2 {
		t.Fatalf("measured rejection=%+v", observation)
	}
	stableJSON, err := json.Marshal(makeStableObservationReport(report))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(stableJSON, []byte(`"executionErrorCode":"query_budget"`)) ||
		bytes.Contains(stableJSON, []byte(`"executionError":`)) ||
		bytes.Contains(stableJSON, []byte("index keys examined exceed")) {
		t.Fatalf("stable rejection leaked or lost error evidence: %s", stableJSON)
	}
}

func TestArrayObserverScenariosScalePredicateWorkAcrossBackends(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, backend := range []string{"memory", "durable"} {
		t.Run(backend, func(t *testing.T) {
			short, err := observe(ctx, observerConfig{
				Backend: backend, Documents: 128, Iterations: 1, Warmup: 0, PayloadBytes: 0,
				ArrayItems: 4, ArrayDuplicatePercent: 0, Scenarios: "array-all-miss,array-elem-scalar,array-elem-object",
			})
			if err != nil {
				t.Fatal(err)
			}
			long, err := observe(ctx, observerConfig{
				Backend: backend, Documents: 128, Iterations: 1, Warmup: 0, PayloadBytes: 0,
				ArrayItems: 32, ArrayDuplicatePercent: 75, Scenarios: "array-all-miss,array-elem-scalar,array-elem-object",
			})
			if err != nil {
				t.Fatal(err)
			}
			if short.Config.ArrayItems != 4 || long.Config.ArrayItems != 32 ||
				short.Config.ArrayDuplicatePercent != 0 || long.Config.ArrayDuplicatePercent != 75 {
				t.Fatalf("array config short=%+v long=%+v", short.Config, long.Config)
			}
			shortByName, longByName := observationsByName(short), observationsByName(long)
			for _, name := range []string{"array-all-miss", "array-elem-scalar", "array-elem-object"} {
				before, after := shortByName[name], longByName[name]
				if before.Parameters == nil || after.Parameters == nil || before.Parameters.ArrayItems == nil || after.Parameters.ArrayItems == nil ||
					*before.Parameters.ArrayItems != 4 || *after.Parameters.ArrayItems != 32 ||
					after.Explain.DocumentsExamined != before.Explain.DocumentsExamined ||
					after.Explain.Budget.PredicateStepsUsed <= before.Explain.Budget.PredicateStepsUsed ||
					after.Measured.PredicateSteps <= before.Measured.PredicateSteps {
					t.Fatalf("%s short=%+v long=%+v", name, before, after)
				}
			}
		})
	}
}

func TestMatrixProfileMeasuresOverlapAndBackendLimitDifferences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reports, err := observeReports(ctx, observerConfig{
		Backend: "both", Profile: matrixProfile, Documents: 640, Iterations: 1, Warmup: 0,
		PayloadBytes: 0, Scenarios: "all",
		MatrixOverlaps: []int{0, 10, 25, 50},
		MatrixLimits:   []string{"1", "10", "100", "none"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("reports=%d", len(reports))
	}
	byBackend := make(map[string]observationReport, len(reports))
	for _, report := range reports {
		byBackend[report.Config.Backend] = report
		if report.Config.Profile != matrixProfile || len(report.Scenarios) != 8 ||
			report.MeasuredTotals.Total != 8 || report.MeasuredTotals.Failed != 0 {
			t.Fatalf("%s report=%+v", report.Config.Backend, report)
		}
		observations := observationsByName(report)
		expectedDuplicates := map[string]int64{
			"or-overlap-00": 0,
			"or-overlap-10": 64,
			"or-overlap-25": 160,
			"or-overlap-50": 320,
		}
		for name, duplicateIDs := range expectedDuplicates {
			observation := observations[name]
			if observation.Family != "or-overlap" || observation.Parameters == nil ||
				observation.Explain.PlanReason != "multi_index_union" ||
				observation.Explain.KeysExamined != 640 ||
				observation.Explain.CandidateIDs != 640 ||
				observation.Explain.DuplicateCandidateIDs != duplicateIDs ||
				observation.Explain.UniqueCandidateIDs != 640-duplicateIDs ||
				observation.Ratios.DuplicateCandidatePercent != float64(duplicateIDs)/640*100 {
				t.Fatalf("%s/%s=%+v", report.Config.Backend, name, observation)
			}
		}
		if hasAdvice(observations["or-overlap-10"].Explain.Advice, "high_union_overlap") ||
			!hasAdvice(observations["or-overlap-25"].Explain.Advice, "high_union_overlap") ||
			!hasAdvice(observations["or-overlap-50"].Explain.Advice, "high_union_overlap") {
			t.Fatalf("%s overlap advice=%+v", report.Config.Backend, observations)
		}
		if observations["limit-001"].ReturnedTotal != 1 ||
			observations["limit-010"].ReturnedTotal != 10 ||
			observations["limit-100"].ReturnedTotal != 10 ||
			observations["limit-none"].ReturnedTotal != 10 {
			t.Fatalf("%s limit returns=%+v", report.Config.Backend, observations)
		}
		if !observations["limit-001"].Explain.EarlyStopped ||
			observations["limit-100"].Explain.EarlyStopped ||
			observations["limit-none"].Explain.EarlyStopReason != "limit_not_set" {
			t.Fatalf("%s limit early-stop=%+v", report.Config.Backend, observations)
		}
	}

	memory := observationsByName(byBackend["memory"])
	durable := observationsByName(byBackend["durable"])
	if memory["limit-001"].Explain.KeysExamined != 10 ||
		memory["limit-001"].Explain.EarlyStopScope != "documents" {
		t.Fatalf("memory limit-001=%+v", memory["limit-001"])
	}
	if durable["limit-001"].Explain.KeysExamined != 1 ||
		durable["limit-001"].Explain.EarlyStopScope != "keys_and_documents" {
		t.Fatalf("durable limit-001=%+v", durable["limit-001"])
	}
	var comparison bytes.Buffer
	if err := writeTableReports(&comparison, reports); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(comparison.String(), "BACKEND STRUCTURAL COMPARISON") ||
		!strings.Contains(comparison.String(), "limit-001") ||
		!strings.Contains(comparison.String(), "-9") {
		t.Fatalf("comparison table missing structural delta:\n%s", comparison.String())
	}
}

func TestStableJSONIsByteDeterministicAndExcludesTiming(t *testing.T) {
	args := []string{
		"-profile", "matrix", "-backend", "both",
		"-documents", "640", "-iterations", "1", "-warmup", "0",
		"-payload-bytes", "0", "-overlaps", "0,25", "-limits", "1,none",
		"-format", "json-stable",
	}
	var first, second, diagnostics bytes.Buffer
	if err := runCLI(args, &first, &diagnostics); err != nil {
		t.Fatal(err)
	}
	diagnostics.Reset()
	if err := runCLI(args, &second, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Fatalf("stable JSON changed between identical runs\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}
	for _, forbidden := range []string{"setupMillis", "elapsedMillis", "averageLatencyMicros", "databasePath", "explainError\""} {
		if strings.Contains(first.String(), forbidden) {
			t.Fatalf("stable JSON contains %q:\n%s", forbidden, first.String())
		}
	}
	for _, expected := range []string{`"format": "stable"`, `"backend": "memory"`, `"backend": "durable"`, `"targetOverlapPercent": 25`, `"limitMode": "none"`} {
		if !strings.Contains(first.String(), expected) {
			t.Fatalf("stable JSON missing %q:\n%s", expected, first.String())
		}
	}
}

func TestObserverRefusesExistingDurableDatabasePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.meld2")
	const original = "operator-owned"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := openObserverDatabase(observerConfig{Backend: "durable", DatabasePath: path})
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("error=%v", err)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil || string(content) != original {
		t.Fatalf("existing path changed: content=%q err=%v", content, readErr)
	}
}

func TestObserveReportsRetainsOnlyExplicitDurablePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.meld2")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reports, err := observeReports(ctx, observerConfig{
		Backend: "durable", DatabasePath: path, Documents: 128, Iterations: 1,
		PayloadBytes: 0, Scenarios: "indexed-limit",
	})
	if err != nil {
		t.Fatal(err)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || !reports[0].Config.DatabaseRetained ||
		reports[0].Config.DatabasePath != absolute {
		t.Fatalf("retained report=%+v", reports)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("retained database missing: %v", err)
	}
}

func TestObserverOutputFormats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := observe(ctx, observerConfig{
		Backend: "memory", Documents: 128, Iterations: 1, Warmup: 0,
		PayloadBytes: 0, Scenarios: "indexed-limit,overlapping-or",
	})
	if err != nil {
		t.Fatal(err)
	}
	var table bytes.Buffer
	if err := writeTableReport(&table, report); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"EXPLAIN", "ACCESS SOURCES", "MEASURED FIND RUNS", "high_union_overlap"} {
		if !strings.Contains(table.String(), expected) {
			t.Fatalf("table missing %q:\n%s", expected, table.String())
		}
	}

	var output, diagnostics bytes.Buffer
	if err := runCLI([]string{
		"-documents", "128", "-iterations", "1", "-warmup", "0",
		"-payload-bytes", "0", "-scenarios", "indexed-limit", "-format", "json",
	}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"schemaVersion": 1`, `"indexed-limit"`, `"measuredTotals"`} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("JSON missing %q:\n%s", expected, output.String())
		}
	}
}

func TestParseObserverConfigRejectsUnsafeOrInvalidInputs(t *testing.T) {
	var diagnostics bytes.Buffer
	if _, err := parseObserverConfig([]string{"-backend", "memory", "-database", "observer.meld2"}, &diagnostics); err == nil {
		t.Fatal("memory backend accepted a database path")
	}
	if _, err := parseObserverConfig([]string{"-documents", "10"}, &diagnostics); err == nil {
		t.Fatal("accepted too few documents")
	}
	if _, err := parseObserverConfig([]string{"-format", "csv"}, &diagnostics); err == nil {
		t.Fatal("accepted an unsupported format")
	}
	if _, err := parseObserverConfig([]string{"-array-items", "0"}, &diagnostics); err == nil {
		t.Fatal("accepted zero array items")
	}
	if _, err := parseObserverConfig([]string{"-array-duplicate-percent", "91"}, &diagnostics); err == nil {
		t.Fatal("accepted array duplication above 90 percent")
	}
	if _, err := parseObserverConfig([]string{"-profile", "matrix", "-overlaps", "0,51"}, &diagnostics); err == nil {
		t.Fatal("accepted an overlap above 50 percent")
	}
	if _, err := parseObserverConfig([]string{"-profile", "matrix", "-limits", "0,none"}, &diagnostics); err == nil {
		t.Fatal("accepted a zero matrix limit")
	}
	if _, err := parseObserverConfig([]string{"-backend", "both", "-database", "observer.meld2"}, &diagnostics); err == nil {
		t.Fatal("both backends accepted a retained database path")
	}
}

func observationsByName(report observationReport) map[string]scenarioObservation {
	observations := make(map[string]scenarioObservation, len(report.Scenarios))
	for _, observation := range report.Scenarios {
		observations[observation.Name] = observation
	}
	return observations
}

func hasAdvice(advice []meldbase.ExplainAdvice, code string) bool {
	for _, item := range advice {
		if item.Code == code {
			return true
		}
	}
	return false
}
