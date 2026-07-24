package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/crapthings/meldbase"
)

const (
	reportSchemaVersion = 1
	observerCollection  = "query_observer"
	observerBucketCount = 64
	standardProfile     = "standard"
	matrixProfile       = "matrix"
)

var (
	defaultMatrixOverlaps = []int{0, 10, 25, 50}
	defaultMatrixLimits   = []string{"1", "10", "100", "none"}
)

type observerConfig struct {
	Backend               string
	DatabasePath          string
	Profile               string
	Documents             int
	Iterations            int
	Warmup                int
	PayloadBytes          int
	ArrayItems            int
	ArrayDuplicatePercent int
	Scenarios             string
	MatrixOverlaps        []int
	MatrixLimits          []string
	Format                string
	Timeout               time.Duration
	Limits                meldbase.ResourceLimits
}

type reportConfig struct {
	Backend               string                  `json:"backend"`
	DatabasePath          string                  `json:"databasePath,omitempty"`
	DatabaseRetained      bool                    `json:"databaseRetained"`
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

type observationReport struct {
	SchemaVersion  int                   `json:"schemaVersion"`
	Config         reportConfig          `json:"config"`
	SetupMillis    float64               `json:"setupMillis"`
	Scenarios      []scenarioObservation `json:"scenarios"`
	MeasuredTotals queryCounters         `json:"measuredTotals"`
}

type scenarioObservation struct {
	Name                 string                 `json:"name"`
	Family               string                 `json:"family"`
	Intent               string                 `json:"intent"`
	Parameters           *scenarioParameters    `json:"parameters,omitempty"`
	Explain              meldbase.ExplainResult `json:"explain"`
	ExplainErrorCode     string                 `json:"explainErrorCode,omitempty"`
	ExplainError         string                 `json:"explainError,omitempty"`
	WarmupFailures       int                    `json:"warmupFailures"`
	RunsAttempted        int                    `json:"runsAttempted"`
	RunsFailed           int                    `json:"runsFailed"`
	ExecutionErrorCode   string                 `json:"executionErrorCode,omitempty"`
	ExecutionError       string                 `json:"executionError,omitempty"`
	ReturnedTotal        int                    `json:"returnedTotal"`
	AverageReturned      float64                `json:"averageReturned"`
	ElapsedMillis        float64                `json:"elapsedMillis"`
	AverageLatencyMicros float64                `json:"averageLatencyMicros"`
	Measured             queryCounters          `json:"measured"`
	Ratios               explainRatios          `json:"ratios"`
}

type explainRatios struct {
	KeysPerUniqueCandidate    float64 `json:"keysPerUniqueCandidate"`
	KeysPerDocumentExamined   float64 `json:"keysPerDocumentExamined"`
	DocumentsPerRetained      float64 `json:"documentsPerRetainedCandidate"`
	DuplicateCandidatePercent float64 `json:"duplicateCandidatePercent"`
	PredicateStepsPerDocument float64 `json:"predicateStepsPerDocumentExamined"`
	PredicateStepsPerRetained float64 `json:"predicateStepsPerRetainedCandidate"`
	PeakBudgetResource        string  `json:"peakBudgetResource,omitempty"`
	PeakBudgetUtilizationPct  float64 `json:"peakBudgetUtilizationPercent"`
}

// queryCounters deliberately excludes ActiveCursors because it is a gauge,
// while every field below is a monotonic process-session counter.
type queryCounters struct {
	Total                 uint64 `json:"total"`
	Failed                uint64 `json:"failed"`
	CollectionScans       uint64 `json:"collectionScans"`
	IndexScans            uint64 `json:"indexScans"`
	IDLookups             uint64 `json:"idLookups"`
	DocumentsExamined     uint64 `json:"documentsExamined"`
	DocumentsReturned     uint64 `json:"documentsReturned"`
	KeysExamined          uint64 `json:"keysExamined"`
	PredicateSteps        uint64 `json:"predicateSteps"`
	CandidateIDs          uint64 `json:"candidateIds"`
	UniqueCandidateIDs    uint64 `json:"uniqueCandidateIds"`
	DuplicateCandidateIDs uint64 `json:"duplicateCandidateIds"`
	CandidatesRetained    uint64 `json:"candidatesRetained"`
	SortBytes             uint64 `json:"sortBytes"`
	EarlyStops            uint64 `json:"earlyStops"`
	BudgetPressureEvents  uint64 `json:"budgetPressureEvents"`
	BudgetRejections      uint64 `json:"budgetRejections"`
}

type observerScenario struct {
	name       string
	family     string
	intent     string
	parameters scenarioParameters
	query      meldbase.QuerySpec
}

type observerIndex struct {
	name   string
	fields []meldbase.IndexField
}

type scenarioParameters struct {
	TargetOverlapPercent  *int   `json:"targetOverlapPercent,omitempty"`
	Limit                 *int   `json:"limit,omitempty"`
	LimitMode             string `json:"limitMode,omitempty"`
	ArrayItems            *int   `json:"arrayItems,omitempty"`
	ArrayDuplicatePercent *int   `json:"arrayDuplicatePercent,omitempty"`
}

type observerDatabase struct {
	db           *meldbase.DB
	retainedPath string
	cleanup      func() error
}

func observe(ctx context.Context, config observerConfig) (report observationReport, resultErr error) {
	profile := normalizedObserverProfile(config.Profile)
	matrixOverlaps := normalizedMatrixOverlaps(config.MatrixOverlaps)
	matrixLimits := normalizedMatrixLimits(config.MatrixLimits)
	arrayItems := config.ArrayItems
	if arrayItems == 0 {
		arrayItems = 16
	}
	allScenarios, err := buildConfiguredObserverScenarios(config.Documents, profile, matrixOverlaps, matrixLimits, arrayItems, config.ArrayDuplicatePercent)
	if err != nil {
		return observationReport{}, err
	}
	scenarios, err := selectObserverScenarios(allScenarios, config.Scenarios)
	if err != nil {
		return observationReport{}, err
	}

	handle, err := openObserverDatabase(config)
	if err != nil {
		return observationReport{}, err
	}
	defer func() {
		if err := handle.cleanup(); resultErr == nil {
			resultErr = err
		}
	}()

	setupStarted := time.Now()
	collection := handle.db.Collection(observerCollection)
	if err := seedObserverCollection(ctx, collection, config.Documents, config.PayloadBytes, profile, matrixOverlaps, arrayItems, config.ArrayDuplicatePercent); err != nil {
		return observationReport{}, fmt.Errorf("seed observer collection: %w", err)
	}
	if err := createObserverIndexes(ctx, collection, profile, matrixOverlaps); err != nil {
		return observationReport{}, fmt.Errorf("create observer indexes: %w", err)
	}
	setupMillis := durationMillis(time.Since(setupStarted))

	report = observationReport{
		SchemaVersion: reportSchemaVersion,
		Config: reportConfig{
			Backend:               config.Backend,
			DatabasePath:          handle.retainedPath,
			DatabaseRetained:      handle.retainedPath != "",
			Profile:               profile,
			Documents:             config.Documents,
			Iterations:            config.Iterations,
			Warmup:                config.Warmup,
			PayloadBytes:          config.PayloadBytes,
			ArrayItems:            arrayItems,
			ArrayDuplicatePercent: config.ArrayDuplicatePercent,
			Scenarios:             make([]string, 0, len(scenarios)),
			Limits:                handle.db.ResourceLimits(),
		},
		SetupMillis: setupMillis,
		Scenarios:   make([]scenarioObservation, 0, len(scenarios)),
	}
	if profile == matrixProfile {
		report.Config.MatrixOverlaps = append([]int(nil), matrixOverlaps...)
		report.Config.MatrixLimits = append([]string(nil), matrixLimits...)
	}
	for _, scenario := range scenarios {
		report.Config.Scenarios = append(report.Config.Scenarios, scenario.name)
		observation := observeScenario(ctx, handle.db, collection, scenario, config)
		report.Scenarios = append(report.Scenarios, observation)
		report.MeasuredTotals = addQueryCounters(report.MeasuredTotals, observation.Measured)
	}
	return report, nil
}

func openObserverDatabase(config observerConfig) (observerDatabase, error) {
	switch config.Backend {
	case "memory":
		if config.DatabasePath != "" {
			return observerDatabase{}, fmt.Errorf("-database is only valid with -backend durable")
		}
		db, err := meldbase.NewWithOptions(meldbase.DatabaseOptions{ResourceLimits: config.Limits})
		if err != nil {
			return observerDatabase{}, err
		}
		return observerDatabase{db: db, cleanup: db.Close}, nil
	case "durable":
		return openDurableObserverDatabase(config)
	default:
		return observerDatabase{}, fmt.Errorf("unsupported backend %q", config.Backend)
	}
}

func openDurableObserverDatabase(config observerConfig) (observerDatabase, error) {
	path := config.DatabasePath
	temporaryDirectory := ""
	if path == "" {
		directory, err := os.MkdirTemp("", "meldbase-query-observer-")
		if err != nil {
			return observerDatabase{}, err
		}
		temporaryDirectory = directory
		path = filepath.Join(directory, "observer.meld2")
	} else {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return observerDatabase{}, err
		}
		path = absolute
		if _, err := os.Lstat(path); err == nil {
			return observerDatabase{}, fmt.Errorf("refusing to overwrite existing database %q", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return observerDatabase{}, err
		}
		parent, err := os.Stat(filepath.Dir(path))
		if err != nil {
			return observerDatabase{}, err
		}
		if !parent.IsDir() {
			return observerDatabase{}, fmt.Errorf("database parent is not a directory: %q", filepath.Dir(path))
		}
	}

	db, err := meldbase.OpenWithOptions(path, meldbase.OpenOptions{ResourceLimits: config.Limits})
	if err != nil {
		if temporaryDirectory != "" {
			_ = os.RemoveAll(temporaryDirectory)
		}
		return observerDatabase{}, err
	}
	retainedPath := ""
	if config.DatabasePath != "" {
		retainedPath = path
	}
	return observerDatabase{
		db:           db,
		retainedPath: retainedPath,
		cleanup: func() error {
			closeErr := db.Close()
			if temporaryDirectory == "" {
				return closeErr
			}
			return errors.Join(closeErr, os.RemoveAll(temporaryDirectory))
		},
	}, nil
}

func seedObserverCollection(
	ctx context.Context,
	collection *meldbase.Collection,
	documentCount, payloadBytes int,
	profile string,
	matrixOverlaps []int,
	arrayItems, arrayDuplicatePercent int,
) error {
	const batchSize = 512
	payload := meldbase.String(strings.Repeat("x", payloadBytes))
	groups := make([]meldbase.Value, 16)
	for index := range groups {
		groups[index] = meldbase.String(fmt.Sprintf("group-%02d", index))
	}
	active := meldbase.String("active")
	inactive := meldbase.String("inactive")

	for first := 0; first < documentCount; first += batchSize {
		last := min(first+batchSize, documentCount)
		documents := make([]meldbase.Document, 0, last-first)
		for index := first; index < last; index++ {
			state := inactive
			if index%2 == 0 {
				state = active
			}
			rank := int64((uint64(index)*7_919 + 17) % uint64(documentCount))
			document := meldbase.Document{
				"bucket":  meldbase.Int(int64(index % observerBucketCount)),
				"state":   state,
				"score":   meldbase.Int(int64(index)),
				"rank":    meldbase.Int(rank),
				"group":   groups[index%len(groups)],
				"payload": payload,
			}
			if profile == matrixProfile {
				document["or_left"] = meldbase.Bool(index < documentCount/2)
				for _, overlap := range matrixOverlaps {
					document[matrixRightField(overlap)] = meldbase.Bool(matrixRightMember(index, documentCount, overlap))
				}
			} else {
				status := "closed"
				if (index/8)%8 == 0 {
					status = "open"
				}
				document["workspaceId"] = meldbase.String(fmt.Sprintf("workspace-%02d", index%8))
				document["status"] = meldbase.String(status)
				document["compoundStatus"] = meldbase.String(status)
				tags, scores, parts := observerArrayPredicateValues(index, arrayItems, arrayDuplicatePercent)
				document["arrayTags"] = tags
				document["arrayScores"] = scores
				document["arrayParts"] = parts
			}
			documents = append(documents, document)
		}
		if _, err := collection.InsertMany(ctx, documents); err != nil {
			return err
		}
	}
	return nil
}

func observerArrayPredicateValues(index, items, duplicatePercent int) (meldbase.Value, meldbase.Value, meldbase.Value) {
	unique := max(1, items*(100-duplicatePercent)/100)
	tags := make([]meldbase.Value, 0, items)
	scores := make([]meldbase.Value, 0, items)
	parts := make([]meldbase.Value, 0, items)
	for offset := 0; offset < items; offset++ {
		tags = append(tags, meldbase.String(fmt.Sprintf("tag-%02d", (index+offset%unique)%64)))
		score := int64((index + offset) % 64)
		kind := "other"
		rank := int64(1)
		if offset == items-1 {
			if index%4 == 0 {
				score, kind, rank = 85, "target", 7
			} else if index%4 == 1 {
				kind, rank = "target", 1
			} else {
				rank = 7
			}
		}
		scores = append(scores, meldbase.Int(score))
		parts = append(parts, meldbase.Object(meldbase.Document{"kind": meldbase.String(kind), "rank": meldbase.Int(rank)}))
	}
	return meldbase.Array(tags...), meldbase.Array(scores...), meldbase.Array(parts...)
}

func createObserverIndexes(ctx context.Context, collection *meldbase.Collection, profile string, matrixOverlaps []int) error {
	indexes := []observerIndex{singleObserverIndex("by_bucket", "bucket")}
	if profile == matrixProfile {
		indexes = append(indexes, singleObserverIndex("by_or_left", "or_left"))
		for _, overlap := range matrixOverlaps {
			indexes = append(indexes, singleObserverIndex(matrixRightIndex(overlap), matrixRightField(overlap)))
		}
	} else {
		indexes = append(indexes,
			singleObserverIndex("by_score", "score"),
			singleObserverIndex("by_state", "state"),
			singleObserverIndex("by_status", "status"),
			singleObserverIndex("by_workspace", "workspaceId"),
			singleObserverIndex("by_compound_status", "compoundStatus"),
			observerIndex{
				name: "by_workspace_compound_status",
				fields: []meldbase.IndexField{
					{Field: "workspaceId", Order: 1},
					{Field: "compoundStatus", Order: 1},
				},
			},
		)
	}
	for _, index := range indexes {
		if err := collection.CreateIndex(ctx, index.name, index.fields, meldbase.IndexOptions{}); err != nil {
			return fmt.Errorf("%s: %w", index.name, err)
		}
	}
	return nil
}

func singleObserverIndex(name, field string) observerIndex {
	return observerIndex{name: name, fields: []meldbase.IndexField{{Field: field, Order: 1}}}
}

func buildConfiguredObserverScenarios(documentCount int, profile string, matrixOverlaps []int, matrixLimits []string, arrayItems, arrayDuplicatePercent int) ([]observerScenario, error) {
	switch profile {
	case standardProfile:
		return buildObserverScenarios(documentCount, arrayItems, arrayDuplicatePercent)
	case matrixProfile:
		if err := validateMatrixOverlaps(matrixOverlaps); err != nil {
			return nil, err
		}
		if err := validateMatrixLimits(matrixLimits); err != nil {
			return nil, err
		}
		return buildMatrixObserverScenarios(matrixOverlaps, matrixLimits)
	default:
		return nil, fmt.Errorf("unsupported profile %q", profile)
	}
}

func buildObserverScenarios(documentCount, arrayItems, arrayDuplicatePercent int) ([]observerScenario, error) {
	window := min(20, max(1, documentCount/(observerBucketCount*2)))
	halfBuckets := make([]any, observerBucketCount/2)
	for index := range halfBuckets {
		halfBuckets[index] = int64(index)
	}
	lower := int64(documentCount / 4)
	upper := int64(documentCount * 3 / 4)

	definitions := []struct {
		name       string
		family     string
		intent     string
		parameters scenarioParameters
		filter     meldbase.Filter
		options    meldbase.QueryOptions
	}{
		{
			name: "indexed-limit", family: "limit", intent: "exact secondary lookup with a proven unsorted limit stop",
			parameters: scenarioParameters{Limit: intPointer(window), LimitMode: "bounded"},
			filter:     meldbase.Filter{"bucket": int64(7)}, options: meldbase.QueryOptions{Limit: intPointer(window)},
		},
		{
			name: "overlapping-or", family: "or-overlap", intent: "cross-index union with deliberately overlapping branches",
			filter: meldbase.Filter{"$or": []meldbase.Filter{
				{"bucket": map[string]any{"$in": halfBuckets}},
				{"state": "active"},
			}},
		},
		{
			name: "range-limit", family: "range", intent: "range scan whose limit cannot safely stop physical key work",
			parameters: scenarioParameters{Limit: intPointer(window), LimitMode: "bounded"},
			filter:     meldbase.Filter{"score": map[string]any{"$gte": lower, "$lt": upper}},
			options:    meldbase.QueryOptions{Limit: intPointer(window)},
		},
		{
			name: "sort-pressure", family: "sort", intent: "indexed filtering followed by an unindexed in-memory sort",
			filter:  meldbase.Filter{"state": "active"},
			options: meldbase.QueryOptions{Sort: []meldbase.SortField{{Path: "rank", Direction: 1}}},
		},
		{
			name: "collection-scan", family: "collection-scan", intent: "unindexed predicate that must inspect the collection",
			filter: meldbase.Filter{"group": "group-03"},
		},
		{
			name: "and-separate-indexes", family: "and-access", intent: "two independently indexed predicates using one source plus residual filtering",
			filter: meldbase.Filter{"workspaceId": "workspace-01", "status": "open"},
		},
		{
			name: "and-compound-index", family: "and-access", intent: "the same selectivity shape constrained by a compound index",
			filter: meldbase.Filter{"workspaceId": "workspace-01", "compoundStatus": "open"},
		},
		{
			name: "array-all-miss", family: "array-all", intent: "indexed admission followed by a $all residual that scans every array element before rejecting",
			parameters: scenarioParameters{ArrayItems: intPointer(arrayItems), ArrayDuplicatePercent: intPointer(arrayDuplicatePercent)},
			filter:     meldbase.Filter{"state": "active", "arrayTags": map[string]any{"$all": []any{"tag-not-present", "tag-00", "tag-not-present"}}},
		},
		{
			name: "array-elem-scalar", family: "array-elem-match", intent: "indexed admission followed by scalar $elemMatch with its qualifying value at the array tail",
			parameters: scenarioParameters{ArrayItems: intPointer(arrayItems), ArrayDuplicatePercent: intPointer(arrayDuplicatePercent)},
			filter:     meldbase.Filter{"state": "active", "arrayScores": map[string]any{"$elemMatch": map[string]any{"$gte": int64(80), "$lt": int64(90)}}},
		},
		{
			name: "array-elem-object", family: "array-elem-match", intent: "indexed admission followed by object $elemMatch that proves conditions share one array element",
			parameters: scenarioParameters{ArrayItems: intPointer(arrayItems), ArrayDuplicatePercent: intPointer(arrayDuplicatePercent)},
			filter:     meldbase.Filter{"state": "active", "arrayParts": map[string]any{"$elemMatch": map[string]any{"kind": "target", "rank": map[string]any{"$gte": int64(5)}}}},
		},
	}

	scenarios := make([]observerScenario, 0, len(definitions))
	for _, definition := range definitions {
		query, err := meldbase.CompileQuery(definition.filter, definition.options)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", definition.name, err)
		}
		scenarios = append(scenarios, observerScenario{
			name: definition.name, family: definition.family, intent: definition.intent,
			parameters: definition.parameters, query: query,
		})
	}
	return scenarios, nil
}

func buildMatrixObserverScenarios(overlaps []int, limits []string) ([]observerScenario, error) {
	scenarios := make([]observerScenario, 0, len(overlaps)+len(limits))
	for _, overlap := range overlaps {
		target := overlap
		name := fmt.Sprintf("or-overlap-%02d", overlap)
		query, err := meldbase.CompileQuery(meldbase.Filter{"$or": []meldbase.Filter{
			{"or_left": true},
			{matrixRightField(overlap): true},
		}}, meldbase.QueryOptions{})
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", name, err)
		}
		scenarios = append(scenarios, observerScenario{
			name: name, family: "or-overlap",
			intent:     fmt.Sprintf("two equally sized index branches targeting %d%% duplicate candidates", overlap),
			parameters: scenarioParameters{TargetOverlapPercent: &target},
			query:      query,
		})
	}
	for _, rawLimit := range limits {
		name := "limit-none"
		options := meldbase.QueryOptions{}
		parameters := scenarioParameters{LimitMode: "none"}
		intent := "exact secondary lookup without a result limit"
		if rawLimit != "none" {
			limit, err := parseMatrixLimit(rawLimit)
			if err != nil {
				return nil, err
			}
			name = fmt.Sprintf("limit-%03d", limit)
			options.Limit = intPointer(limit)
			parameters = scenarioParameters{Limit: intPointer(limit), LimitMode: "bounded"}
			intent = fmt.Sprintf("exact secondary lookup with limit %d", limit)
		}
		query, err := meldbase.CompileQuery(meldbase.Filter{"bucket": int64(7)}, options)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", name, err)
		}
		scenarios = append(scenarios, observerScenario{
			name: name, family: "limit", intent: intent, parameters: parameters, query: query,
		})
	}
	return scenarios, nil
}

func selectObserverScenarios(all []observerScenario, requested string) ([]observerScenario, error) {
	if requested == "" || requested == "all" {
		return append([]observerScenario(nil), all...), nil
	}
	available := make(map[string]observerScenario, len(all))
	for _, scenario := range all {
		available[scenario.name] = scenario
	}
	selected := make([]observerScenario, 0)
	seen := make(map[string]struct{})
	for _, raw := range strings.Split(requested, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, fmt.Errorf("empty scenario in %q", requested)
		}
		scenario, exists := available[name]
		if !exists {
			return nil, fmt.Errorf("unknown scenario %q", name)
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		selected = append(selected, scenario)
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no scenarios selected")
	}
	return selected, nil
}

func observeScenario(ctx context.Context, db *meldbase.DB, collection *meldbase.Collection, scenario observerScenario, config observerConfig) scenarioObservation {
	observation := scenarioObservation{
		Name: scenario.name, Family: scenario.family, Intent: scenario.intent,
		Parameters: cloneScenarioParameters(scenario.parameters),
	}
	explain, explainErr := collection.ExplainQuery(ctx, scenario.query)
	observation.Explain = explain
	observation.Ratios = calculateExplainRatios(explain)
	if explainErr != nil {
		observation.ExplainErrorCode = observerErrorCode(explainErr)
		observation.ExplainError = explainErr.Error()
	}

	for range config.Warmup {
		if _, err := executeObserverQuery(ctx, collection, scenario.query); err != nil {
			observation.WarmupFailures++
			if observation.ExecutionError == "" {
				observation.ExecutionErrorCode = observerErrorCode(err)
				observation.ExecutionError = err.Error()
			}
		}
		if ctx.Err() != nil {
			break
		}
	}

	before := db.Stats().Queries
	started := time.Now()
	for range config.Iterations {
		if ctx.Err() != nil {
			break
		}
		observation.RunsAttempted++
		returned, err := executeObserverQuery(ctx, collection, scenario.query)
		observation.ReturnedTotal += returned
		if err != nil {
			observation.RunsFailed++
			observation.ExecutionErrorCode = observerErrorCode(err)
			observation.ExecutionError = err.Error()
		}
	}
	elapsed := time.Since(started)
	after := db.Stats().Queries

	observation.ElapsedMillis = durationMillis(elapsed)
	if observation.RunsAttempted > 0 {
		observation.AverageLatencyMicros = float64(elapsed.Nanoseconds()) / float64(observation.RunsAttempted) / 1_000
		observation.AverageReturned = float64(observation.ReturnedTotal) / float64(observation.RunsAttempted)
	}
	observation.Measured = queryStatsDelta(before, after)
	return observation
}

func executeObserverQuery(ctx context.Context, collection *meldbase.Collection, query meldbase.QuerySpec) (int, error) {
	cursor, err := collection.FindQuery(ctx, query)
	if err != nil {
		return 0, err
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		_ = cursor.Close()
		return len(documents), err
	}
	return len(documents), nil
}

func calculateExplainRatios(explain meldbase.ExplainResult) explainRatios {
	ratios := explainRatios{
		KeysPerUniqueCandidate:    divideInt64(explain.KeysExamined, explain.UniqueCandidateIDs),
		KeysPerDocumentExamined:   divideInt64(explain.KeysExamined, explain.DocumentsExamined),
		DocumentsPerRetained:      divideInt64(explain.DocumentsExamined, int64(explain.CandidatesRetained)),
		DuplicateCandidatePercent: percentInt64(explain.DuplicateCandidateIDs, explain.CandidateIDs),
		PredicateStepsPerDocument: divideUint64ByInt64(explain.Budget.PredicateStepsUsed, explain.DocumentsExamined),
		PredicateStepsPerRetained: divideUint64(explain.Budget.PredicateStepsUsed, explain.CandidatesRetained),
	}
	budgets := []struct {
		name        string
		used, limit uint64
	}{
		{name: "documents", used: explain.Budget.DocumentsUsed, limit: explain.Budget.DocumentsLimit},
		{name: "keys", used: explain.Budget.KeysUsed, limit: explain.Budget.KeysLimit},
		{name: "candidates", used: explain.Budget.CandidatesUsed, limit: explain.Budget.CandidatesLimit},
		{name: "sort_bytes", used: explain.Budget.SortBytesUsed, limit: explain.Budget.SortBytesLimit},
		{name: "skip", used: explain.Budget.SkipUsed, limit: explain.Budget.SkipLimit},
		{name: "predicate_steps", used: explain.Budget.PredicateStepsUsed, limit: explain.Budget.PredicateStepsLimit},
	}
	for _, budget := range budgets {
		utilization := percentUint64(budget.used, budget.limit)
		if ratios.PeakBudgetResource == "" || utilization > ratios.PeakBudgetUtilizationPct {
			ratios.PeakBudgetResource = budget.name
			ratios.PeakBudgetUtilizationPct = utilization
		}
	}
	return ratios
}

func observerErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, meldbase.ErrQueryBudget):
		return "query_budget"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "query_error"
	}
}

func queryStatsDelta(before, after meldbase.QueryStats) queryCounters {
	return queryCounters{
		Total:                 counterDelta(before.Total, after.Total),
		Failed:                counterDelta(before.Failed, after.Failed),
		CollectionScans:       counterDelta(before.CollectionScans, after.CollectionScans),
		IndexScans:            counterDelta(before.IndexScans, after.IndexScans),
		IDLookups:             counterDelta(before.IDLookups, after.IDLookups),
		DocumentsExamined:     counterDelta(before.DocumentsExamined, after.DocumentsExamined),
		DocumentsReturned:     counterDelta(before.DocumentsReturned, after.DocumentsReturned),
		KeysExamined:          counterDelta(before.KeysExamined, after.KeysExamined),
		PredicateSteps:        counterDelta(before.PredicateSteps, after.PredicateSteps),
		CandidateIDs:          counterDelta(before.CandidateIDs, after.CandidateIDs),
		UniqueCandidateIDs:    counterDelta(before.UniqueCandidateIDs, after.UniqueCandidateIDs),
		DuplicateCandidateIDs: counterDelta(before.DuplicateCandidateIDs, after.DuplicateCandidateIDs),
		CandidatesRetained:    counterDelta(before.CandidatesRetained, after.CandidatesRetained),
		SortBytes:             counterDelta(before.SortBytes, after.SortBytes),
		EarlyStops:            counterDelta(before.EarlyStops, after.EarlyStops),
		BudgetPressureEvents:  counterDelta(before.BudgetPressureEvents, after.BudgetPressureEvents),
		BudgetRejections:      counterDelta(before.BudgetRejections, after.BudgetRejections),
	}
}

func addQueryCounters(left, right queryCounters) queryCounters {
	return queryCounters{
		Total:                 left.Total + right.Total,
		Failed:                left.Failed + right.Failed,
		CollectionScans:       left.CollectionScans + right.CollectionScans,
		IndexScans:            left.IndexScans + right.IndexScans,
		IDLookups:             left.IDLookups + right.IDLookups,
		DocumentsExamined:     left.DocumentsExamined + right.DocumentsExamined,
		DocumentsReturned:     left.DocumentsReturned + right.DocumentsReturned,
		KeysExamined:          left.KeysExamined + right.KeysExamined,
		PredicateSteps:        left.PredicateSteps + right.PredicateSteps,
		CandidateIDs:          left.CandidateIDs + right.CandidateIDs,
		UniqueCandidateIDs:    left.UniqueCandidateIDs + right.UniqueCandidateIDs,
		DuplicateCandidateIDs: left.DuplicateCandidateIDs + right.DuplicateCandidateIDs,
		CandidatesRetained:    left.CandidatesRetained + right.CandidatesRetained,
		SortBytes:             left.SortBytes + right.SortBytes,
		EarlyStops:            left.EarlyStops + right.EarlyStops,
		BudgetPressureEvents:  left.BudgetPressureEvents + right.BudgetPressureEvents,
		BudgetRejections:      left.BudgetRejections + right.BudgetRejections,
	}
}

func counterDelta(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func divideInt64(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func divideUint64(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func divideUint64ByInt64(numerator uint64, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func percentInt64(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}

func percentUint64(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}

func durationMillis(duration time.Duration) float64 {
	return float64(duration.Nanoseconds()) / 1_000_000
}

func normalizedObserverProfile(profile string) string {
	if profile == "" {
		return standardProfile
	}
	return profile
}

func normalizedMatrixOverlaps(overlaps []int) []int {
	if len(overlaps) == 0 {
		return append([]int(nil), defaultMatrixOverlaps...)
	}
	return append([]int(nil), overlaps...)
}

func normalizedMatrixLimits(limits []string) []string {
	if len(limits) == 0 {
		return append([]string(nil), defaultMatrixLimits...)
	}
	return append([]string(nil), limits...)
}

func parseMatrixOverlaps(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	overlaps := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("invalid overlap %q", part)
		}
		overlaps = append(overlaps, value)
	}
	if err := validateMatrixOverlaps(overlaps); err != nil {
		return nil, err
	}
	return overlaps, nil
}

func validateMatrixOverlaps(overlaps []int) error {
	if len(overlaps) == 0 {
		return fmt.Errorf("at least one overlap is required")
	}
	seen := make(map[int]struct{}, len(overlaps))
	for _, overlap := range overlaps {
		if overlap < 0 || overlap > 50 {
			return fmt.Errorf("overlap %d must be between 0 and 50", overlap)
		}
		if _, duplicate := seen[overlap]; duplicate {
			return fmt.Errorf("duplicate overlap %d", overlap)
		}
		seen[overlap] = struct{}{}
	}
	return nil
}

func parseMatrixLimits(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	limits := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.ToLower(strings.TrimSpace(part))
		if value != "none" {
			limit, err := parseMatrixLimit(value)
			if err != nil {
				return nil, err
			}
			value = strconv.Itoa(limit)
		}
		limits = append(limits, value)
	}
	if err := validateMatrixLimits(limits); err != nil {
		return nil, err
	}
	return limits, nil
}

func validateMatrixLimits(limits []string) error {
	if len(limits) == 0 {
		return fmt.Errorf("at least one limit is required")
	}
	seen := make(map[string]struct{}, len(limits))
	for _, value := range limits {
		if value != "none" {
			if _, err := parseMatrixLimit(value); err != nil {
				return err
			}
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("duplicate limit %q", value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func parseMatrixLimit(raw string) (int, error) {
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > meldbase.DefaultQueryLimits.MaxLimit {
		return 0, fmt.Errorf("limit %q must be an integer between 1 and %d or none", raw, meldbase.DefaultQueryLimits.MaxLimit)
	}
	return limit, nil
}

func matrixRightField(overlap int) string {
	return fmt.Sprintf("or_right_%02d", overlap)
}

func matrixRightIndex(overlap int) string {
	return fmt.Sprintf("by_or_right_%02d", overlap)
}

func matrixRightMember(index, documentCount, overlapPercent int) bool {
	half := documentCount / 2
	candidateIDs := half * 2
	overlap := (candidateIDs*overlapPercent + 50) / 100
	overlap = min(overlap, half)
	outside := half - overlap
	return index < overlap || (index >= half && index < half+outside)
}

func cloneScenarioParameters(parameters scenarioParameters) *scenarioParameters {
	if parameters.TargetOverlapPercent == nil && parameters.Limit == nil && parameters.LimitMode == "" && parameters.ArrayItems == nil && parameters.ArrayDuplicatePercent == nil {
		return nil
	}
	clone := parameters
	if parameters.TargetOverlapPercent != nil {
		value := *parameters.TargetOverlapPercent
		clone.TargetOverlapPercent = &value
	}
	if parameters.Limit != nil {
		value := *parameters.Limit
		clone.Limit = &value
	}
	if parameters.ArrayItems != nil {
		value := *parameters.ArrayItems
		clone.ArrayItems = &value
	}
	if parameters.ArrayDuplicatePercent != nil {
		value := *parameters.ArrayDuplicatePercent
		clone.ArrayDuplicatePercent = &value
	}
	return &clone
}

func intPointer(value int) *int {
	return &value
}
