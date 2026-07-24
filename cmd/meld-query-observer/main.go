package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/crapthings/meldbase"
)

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "meld-query-observer:", err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	config, err := parseObserverConfig(args, stderr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	reports, err := observeReports(ctx, config)
	if err != nil {
		return err
	}
	switch config.Format {
	case "json":
		return writeJSONReports(stdout, reports, false)
	case "json-stable":
		return writeJSONReports(stdout, reports, true)
	case "table":
		return writeTableReports(stdout, reports)
	default:
		return fmt.Errorf("unsupported output format %q", config.Format)
	}
}

func parseObserverConfig(args []string, stderr io.Writer) (observerConfig, error) {
	config := observerConfig{
		Backend:      "memory",
		Profile:      standardProfile,
		Documents:    8_192,
		Iterations:   10,
		Warmup:       1,
		PayloadBytes: 128,
		Scenarios:    "all",
		Format:       "table",
		Timeout:      3 * time.Minute,
	}
	matrixOverlaps := "0,10,25,50"
	matrixLimits := "1,10,100,none"
	flags := flag.NewFlagSet("meld-query-observer", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&config.Backend, "backend", config.Backend, "database backend: memory, durable, or both")
	flags.StringVar(&config.DatabasePath, "database", "", "new durable database path to retain; existing paths are refused")
	flags.StringVar(&config.Profile, "profile", config.Profile, "scenario profile: standard or matrix")
	flags.IntVar(&config.Documents, "documents", config.Documents, "number of deterministic synthetic documents")
	flags.IntVar(&config.Iterations, "iterations", config.Iterations, "measured Find executions per scenario")
	flags.IntVar(&config.Warmup, "warmup", config.Warmup, "unmeasured Find executions per scenario")
	flags.IntVar(&config.PayloadBytes, "payload-bytes", config.PayloadBytes, "synthetic payload bytes per document")
	flags.StringVar(&config.Scenarios, "scenarios", config.Scenarios, "all or a comma-separated scenario list")
	flags.StringVar(&matrixOverlaps, "overlaps", matrixOverlaps, "matrix duplicate percentages, from 0 through 50")
	flags.StringVar(&matrixLimits, "limits", matrixLimits, "matrix limits as positive integers or none")
	flags.StringVar(&config.Format, "format", config.Format, "output format: table, json, or json-stable")
	flags.DurationVar(&config.Timeout, "timeout", config.Timeout, "whole-program timeout")
	flags.Uint64Var(&config.Limits.MaxQueryDocumentsExamined, "max-query-documents", 0, "override MaxQueryDocumentsExamined; zero uses the default")
	flags.Uint64Var(&config.Limits.MaxQueryKeysExamined, "max-query-keys", 0, "override MaxQueryKeysExamined; zero uses the default")
	flags.Uint64Var(&config.Limits.MaxQueryCandidates, "max-query-candidates", 0, "override MaxQueryCandidates; zero uses the default")
	flags.Uint64Var(&config.Limits.MaxQuerySortBytes, "max-query-sort-bytes", 0, "override MaxQuerySortBytes; zero uses the default")
	flags.Uint64Var(&config.Limits.MaxQuerySkip, "max-query-skip", 0, "override MaxQuerySkip; zero uses the default")
	if err := flags.Parse(args); err != nil {
		return observerConfig{}, err
	}
	if flags.NArg() != 0 {
		return observerConfig{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if config.Backend != "memory" && config.Backend != "durable" && config.Backend != "both" {
		return observerConfig{}, fmt.Errorf("-backend must be memory, durable, or both")
	}
	if config.DatabasePath != "" && config.Backend != "durable" {
		return observerConfig{}, fmt.Errorf("-database requires -backend durable")
	}
	if config.Profile != standardProfile && config.Profile != matrixProfile {
		return observerConfig{}, fmt.Errorf("-profile must be standard or matrix")
	}
	if config.Documents < 128 || config.Documents > 1_000_000 {
		return observerConfig{}, fmt.Errorf("-documents must be between 128 and 1000000")
	}
	if config.Iterations < 1 || config.Iterations > 1_000_000 {
		return observerConfig{}, fmt.Errorf("-iterations must be between 1 and 1000000")
	}
	if config.Warmup < 0 || config.Warmup > 1_000_000 {
		return observerConfig{}, fmt.Errorf("-warmup must be between 0 and 1000000")
	}
	if config.PayloadBytes < 0 || config.PayloadBytes > 1<<20 {
		return observerConfig{}, fmt.Errorf("-payload-bytes must be between 0 and 1048576")
	}
	if config.Format != "table" && config.Format != "json" && config.Format != "json-stable" {
		return observerConfig{}, fmt.Errorf("-format must be table, json, or json-stable")
	}
	if config.Timeout <= 0 {
		return observerConfig{}, fmt.Errorf("-timeout must be positive")
	}
	overlaps, err := parseMatrixOverlaps(matrixOverlaps)
	if err != nil {
		return observerConfig{}, fmt.Errorf("-overlaps: %w", err)
	}
	limits, err := parseMatrixLimits(matrixLimits)
	if err != nil {
		return observerConfig{}, fmt.Errorf("-limits: %w", err)
	}
	config.MatrixOverlaps = overlaps
	config.MatrixLimits = limits
	return config, nil
}

func observeReports(ctx context.Context, config observerConfig) ([]observationReport, error) {
	backends := []string{config.Backend}
	if config.Backend == "both" {
		backends = []string{"memory", "durable"}
	}
	reports := make([]observationReport, 0, len(backends))
	for _, backend := range backends {
		backendConfig := config
		backendConfig.Backend = backend
		if config.Backend == "both" {
			backendConfig.DatabasePath = ""
		}
		report, err := observe(ctx, backendConfig)
		if err != nil {
			return nil, fmt.Errorf("%s backend: %w", backend, err)
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func writeJSONReports(output io.Writer, reports []observationReport, stable bool) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if stable {
		stableReports := make([]stableObservationReport, len(reports))
		for index := range reports {
			stableReports[index] = makeStableObservationReport(reports[index])
		}
		if len(stableReports) == 1 {
			return encoder.Encode(stableReports[0])
		}
		return encoder.Encode(stableObservationReportSet{
			SchemaVersion: reportSchemaVersion,
			Format:        "stable",
			Reports:       stableReports,
		})
	}
	if len(reports) == 1 {
		return encoder.Encode(reports[0])
	}
	return encoder.Encode(observationReportSet{SchemaVersion: reportSchemaVersion, Reports: reports})
}

func writeTableReports(output io.Writer, reports []observationReport) error {
	for index, report := range reports {
		if index > 0 {
			if _, err := fmt.Fprintln(output); err != nil {
				return err
			}
		}
		if err := writeTableReport(output, report); err != nil {
			return err
		}
	}
	if len(reports) == 2 {
		return writeBackendComparison(output, reports)
	}
	return nil
}

func writeTableReport(output io.Writer, report observationReport) error {
	if _, err := fmt.Fprintf(
		output,
		"Meldbase query observer\nbackend=%s profile=%s documents=%d iterations=%d warmup=%d setup=%s\nExplain is a separate execution; measured counters exclude Explain and warm-up.\n\n",
		report.Config.Backend,
		report.Config.Profile,
		report.Config.Documents,
		report.Config.Iterations,
		report.Config.Warmup,
		formatDurationMillis(report.SetupMillis),
	); err != nil {
		return err
	}

	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "EXPLAIN\tPLAN\tKEYS\tUNIQUE\tDUP%\tDOCS\tDOC/KEEP\tKEY/UNIQ\tRETAINED\tSORT\tPEAK BUDGET\tEARLY STOP\tADVICE"); err != nil {
		return err
	}
	for _, observation := range report.Scenarios {
		explain := observation.Explain
		if _, err := fmt.Fprintf(
			writer,
			"%s\t%s/%s\t%d\t%d\t%.1f\t%d\t%.2f\t%.2f\t%d\t%s\t%s\t%s\t%s\n",
			observation.Name,
			emptyAs(explain.Stage, "UNKNOWN"),
			emptyAs(explain.PlanReason, "not_planned"),
			explain.KeysExamined,
			explain.UniqueCandidateIDs,
			observation.Ratios.DuplicateCandidatePercent,
			explain.DocumentsExamined,
			observation.Ratios.DocumentsPerRetained,
			observation.Ratios.KeysPerUniqueCandidate,
			explain.CandidatesRetained,
			formatBytes(explain.SortBytes),
			formatBudgetPeak(observation),
			formatEarlyStop(explain),
			formatAdvice(explain.Advice),
		); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(output, "\nACCESS SOURCES"); err != nil {
		return err
	}
	writer = tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "SCENARIO\tSOURCE\tSPANS\tEXACT\tKEYS\tCANDIDATES\tUNIQUE\tDUPLICATE\tDOCS"); err != nil {
		return err
	}
	sourceRows := 0
	for _, observation := range report.Scenarios {
		for _, source := range observation.Explain.Sources {
			sourceRows++
			name := source.IndexName
			if source.Primary {
				name = "_id"
			}
			if _, err := fmt.Fprintf(
				writer,
				"%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
				observation.Name,
				emptyAs(name, "unknown"),
				source.Spans,
				source.ExactSpans,
				source.KeysExamined,
				source.CandidateIDs,
				source.UniqueCandidateIDs,
				source.DuplicateCandidateIDs,
				source.DocumentsExamined,
			); err != nil {
				return err
			}
		}
	}
	if sourceRows == 0 {
		if _, err := fmt.Fprintln(writer, "(none)\t-\t-\t-\t-\t-\t-\t-\t-"); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(output, "\nMEASURED FIND RUNS"); err != nil {
		return err
	}
	writer = tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "SCENARIO\tRUNS\tFAILED\tAVG LATENCY\tAVG RETURNED\tKEYS/RUN\tDOCS/RUN\tDUP/RUN\tSORT/RUN\tPRESSURE\tREJECTIONS"); err != nil {
		return err
	}
	for _, observation := range report.Scenarios {
		runs := float64(max(1, observation.RunsAttempted))
		if _, err := fmt.Fprintf(
			writer,
			"%s\t%d\t%d\t%s\t%.1f\t%.1f\t%.1f\t%.1f\t%s\t%d\t%d\n",
			observation.Name,
			observation.RunsAttempted,
			observation.RunsFailed,
			formatMicros(observation.AverageLatencyMicros),
			observation.AverageReturned,
			float64(observation.Measured.KeysExamined)/runs,
			float64(observation.Measured.DocumentsExamined)/runs,
			float64(observation.Measured.DuplicateCandidateIDs)/runs,
			formatBytes(uint64(float64(observation.Measured.SortBytes)/runs)),
			observation.Measured.BudgetPressureEvents,
			observation.Measured.BudgetRejections,
		); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	totals := report.MeasuredTotals
	_, err := fmt.Fprintf(
		output,
		"\nMeasured totals: queries=%d failed=%d collscans=%d indexscans=%d keys=%d documents=%d duplicates=%d sort=%s earlyStops=%d pressure=%d rejections=%d\n",
		totals.Total,
		totals.Failed,
		totals.CollectionScans,
		totals.IndexScans,
		totals.KeysExamined,
		totals.DocumentsExamined,
		totals.DuplicateCandidateIDs,
		formatBytes(totals.SortBytes),
		totals.EarlyStops,
		totals.BudgetPressureEvents,
		totals.BudgetRejections,
	)
	if err != nil {
		return err
	}
	for _, observation := range report.Scenarios {
		if observation.ExplainError != "" {
			if _, err := fmt.Fprintf(output, "%s Explain error [%s]: %s\n", observation.Name, observation.ExplainErrorCode, observation.ExplainError); err != nil {
				return err
			}
		}
		if observation.ExecutionError != "" {
			if _, err := fmt.Fprintf(output, "%s execution error [%s]: %s\n", observation.Name, observation.ExecutionErrorCode, observation.ExecutionError); err != nil {
				return err
			}
		}
	}
	return nil
}

func formatBudgetPeak(observation scenarioObservation) string {
	explain := observation.Explain
	switch {
	case explain.Budget.Exceeded != "":
		return explain.Budget.Exceeded + " exceeded"
	case explain.Budget.Pressure != "":
		return fmt.Sprintf("%s %.1f%%", explain.Budget.Pressure, observation.Ratios.PeakBudgetUtilizationPct)
	case observation.Ratios.PeakBudgetResource == "":
		return "-"
	default:
		return fmt.Sprintf("%s %.1f%%", observation.Ratios.PeakBudgetResource, observation.Ratios.PeakBudgetUtilizationPct)
	}
}

func formatEarlyStop(explain meldbase.ExplainResult) string {
	reason := emptyAs(explain.EarlyStopReason, "unknown")
	switch {
	case explain.EarlyStopped:
		return "yes/" + emptyAs(explain.EarlyStopScope, "unknown") + "/" + reason
	case explain.EarlyStopEligible:
		return "eligible/" + emptyAs(explain.EarlyStopScope, "unknown") + "/" + reason
	default:
		return "no/" + reason
	}
}

func formatAdvice(advice []meldbase.ExplainAdvice) string {
	if len(advice) == 0 {
		return "-"
	}
	codes := make([]string, 0, len(advice))
	for _, item := range advice {
		codes = append(codes, item.Code)
	}
	return strings.Join(codes, ",")
}

func formatBytes(bytes uint64) string {
	const (
		kib = 1 << 10
		mib = 1 << 20
		gib = 1 << 30
	)
	switch {
	case bytes >= gib:
		return fmt.Sprintf("%.1fGiB", float64(bytes)/gib)
	case bytes >= mib:
		return fmt.Sprintf("%.1fMiB", float64(bytes)/mib)
	case bytes >= kib:
		return fmt.Sprintf("%.1fKiB", float64(bytes)/kib)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

func formatDurationMillis(milliseconds float64) string {
	duration := time.Duration(milliseconds * float64(time.Millisecond))
	if duration < time.Millisecond {
		return duration.Round(time.Microsecond).String()
	}
	return duration.Round(time.Millisecond).String()
}

func formatMicros(microseconds float64) string {
	duration := time.Duration(microseconds * float64(time.Microsecond))
	if duration <= 0 {
		return "0s"
	}
	return duration.Round(time.Microsecond).String()
}

func emptyAs(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
