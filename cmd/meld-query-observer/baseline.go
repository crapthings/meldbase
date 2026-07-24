package main

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/crapthings/meldbase"
)

type observationReportSet struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Reports       []observationReport `json:"reports"`
}

// stableObservationReport deliberately excludes setup/query timings, temporary
// database paths, and free-form error text. Its remaining fields are suitable
// for exact CI comparison for the same observer version, backend, dataset, and
// resource limits.
type stableObservationReport struct {
	SchemaVersion  int                         `json:"schemaVersion"`
	Format         string                      `json:"format"`
	Config         stableReportConfig          `json:"config"`
	Scenarios      []stableScenarioObservation `json:"scenarios"`
	MeasuredTotals queryCounters               `json:"measuredTotals"`
}

type stableObservationReportSet struct {
	SchemaVersion int                       `json:"schemaVersion"`
	Format        string                    `json:"format"`
	Reports       []stableObservationReport `json:"reports"`
}

type stableReportConfig struct {
	Backend               string                  `json:"backend"`
	Profile               string                  `json:"profile"`
	Documents             int                     `json:"documents"`
	Iterations            int                     `json:"iterations"`
	Warmup                int                     `json:"warmup"`
	PayloadBytes          int                     `json:"payloadBytes"`
	ArrayItems            int                     `json:"arrayItems"`
	ArrayDuplicatePercent int                     `json:"arrayDuplicatePercent"`
	Scenarios             []string                `json:"scenarios"`
	MatrixOverlaps        []int                   `json:"matrixOverlaps,omitempty"`
	MatrixLimits          []string                `json:"matrixLimits,omitempty"`
	Limits                meldbase.ResourceLimits `json:"limits"`
}

type stableScenarioObservation struct {
	Name               string                 `json:"name"`
	Family             string                 `json:"family"`
	Parameters         *scenarioParameters    `json:"parameters,omitempty"`
	Explain            meldbase.ExplainResult `json:"explain"`
	ExplainErrorCode   string                 `json:"explainErrorCode,omitempty"`
	WarmupFailures     int                    `json:"warmupFailures"`
	RunsAttempted      int                    `json:"runsAttempted"`
	RunsFailed         int                    `json:"runsFailed"`
	ExecutionErrorCode string                 `json:"executionErrorCode,omitempty"`
	ReturnedTotal      int                    `json:"returnedTotal"`
	Measured           queryCounters          `json:"measured"`
	Ratios             explainRatios          `json:"ratios"`
}

func makeStableObservationReport(report observationReport) stableObservationReport {
	stable := stableObservationReport{
		SchemaVersion: report.SchemaVersion,
		Format:        "stable",
		Config: stableReportConfig{
			Backend:               report.Config.Backend,
			Profile:               report.Config.Profile,
			Documents:             report.Config.Documents,
			Iterations:            report.Config.Iterations,
			Warmup:                report.Config.Warmup,
			PayloadBytes:          report.Config.PayloadBytes,
			ArrayItems:            report.Config.ArrayItems,
			ArrayDuplicatePercent: report.Config.ArrayDuplicatePercent,
			Scenarios:             append([]string(nil), report.Config.Scenarios...),
			MatrixOverlaps:        append([]int(nil), report.Config.MatrixOverlaps...),
			MatrixLimits:          append([]string(nil), report.Config.MatrixLimits...),
			Limits:                report.Config.Limits,
		},
		Scenarios:      make([]stableScenarioObservation, len(report.Scenarios)),
		MeasuredTotals: report.MeasuredTotals,
	}
	for index, observation := range report.Scenarios {
		stable.Scenarios[index] = stableScenarioObservation{
			Name:               observation.Name,
			Family:             observation.Family,
			Parameters:         cloneScenarioParametersPointer(observation.Parameters),
			Explain:            observation.Explain,
			ExplainErrorCode:   observation.ExplainErrorCode,
			WarmupFailures:     observation.WarmupFailures,
			RunsAttempted:      observation.RunsAttempted,
			RunsFailed:         observation.RunsFailed,
			ExecutionErrorCode: observation.ExecutionErrorCode,
			ReturnedTotal:      observation.ReturnedTotal,
			Measured:           observation.Measured,
			Ratios:             observation.Ratios,
		}
	}
	return stable
}

func cloneScenarioParametersPointer(parameters *scenarioParameters) *scenarioParameters {
	if parameters == nil {
		return nil
	}
	return cloneScenarioParameters(*parameters)
}

func writeBackendComparison(output io.Writer, reports []observationReport) error {
	var memory, durable *observationReport
	for index := range reports {
		switch reports[index].Config.Backend {
		case "memory":
			memory = &reports[index]
		case "durable":
			durable = &reports[index]
		}
	}
	if memory == nil || durable == nil {
		return nil
	}
	durableScenarios := make(map[string]scenarioObservation, len(durable.Scenarios))
	for _, observation := range durable.Scenarios {
		durableScenarios[observation.Name] = observation
	}

	if _, err := fmt.Fprintln(output, "\nBACKEND STRUCTURAL COMPARISON"); err != nil {
		return err
	}
	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "SCENARIO\tMEM KEYS\tDUR KEYS\tKEY DELTA\tMEM DOCS\tDUR DOCS\tDOC DELTA\tMEM EARLY\tDUR EARLY"); err != nil {
		return err
	}
	for _, memoryObservation := range memory.Scenarios {
		durableObservation, exists := durableScenarios[memoryObservation.Name]
		if !exists {
			continue
		}
		if _, err := fmt.Fprintf(
			writer,
			"%s\t%d\t%d\t%+d\t%d\t%d\t%+d\t%s\t%s\n",
			memoryObservation.Name,
			memoryObservation.Explain.KeysExamined,
			durableObservation.Explain.KeysExamined,
			durableObservation.Explain.KeysExamined-memoryObservation.Explain.KeysExamined,
			memoryObservation.Explain.DocumentsExamined,
			durableObservation.Explain.DocumentsExamined,
			durableObservation.Explain.DocumentsExamined-memoryObservation.Explain.DocumentsExamined,
			formatEarlyStop(memoryObservation.Explain),
			formatEarlyStop(durableObservation.Explain),
		); err != nil {
			return err
		}
	}
	return writer.Flush()
}
