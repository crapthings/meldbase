package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	qualificationSessionPlanSchema  uint32 = 2
	qualificationSessionEventSchema uint32 = 1
	qualificationSessionDirectory          = ".qualification-session"
	qualificationSessionExecutable         = qualificationSessionDirectory + "/qualification-executable"
)

type qualificationSessionPlan struct {
	SchemaVersion                  uint32                     `json:"schemaVersion"`
	SessionID                      string                     `json:"sessionId"`
	SourceRevision                 string                     `json:"sourceRevision"`
	BuildRevision                  string                     `json:"buildRevision"`
	BuildModified                  bool                       `json:"buildModified"`
	CreatedAt                      time.Time                  `json:"createdAt"`
	GOOS                           string                     `json:"goos"`
	GOARCH                         string                     `json:"goarch"`
	GoVersion                      string                     `json:"goVersion"`
	PlatformClass                  string                     `json:"platformClass"`
	ArtifactsRoot                  string                     `json:"artifactsRoot"`
	ExecutableRelativePath         string                     `json:"executableRelativePath"`
	ExecutableBytes                uint64                     `json:"executableBytes"`
	ExecutableSHA256               string                     `json:"executableSha256"`
	EnvironmentRelativePath        string                     `json:"environmentRelativePath"`
	EnvironmentSHA256              string                     `json:"environmentSha256"`
	Volume                         destructiveVolumeReceipt   `json:"volume"`
	ControllerMethod               string                     `json:"controllerMethod"`
	ControllerPublicKeySHA256      string                     `json:"controllerPublicKeySha256,omitempty"`
	ControllerTargetIdentitySHA256 string                     `json:"controllerTargetIdentitySha256,omitempty"`
	Steps                          []qualificationSessionStep `json:"steps"`
}

type qualificationSessionStep struct {
	Ordinal             int    `json:"ordinal"`
	ID                  string `json:"id"`
	Kind                string `json:"kind"`
	PowerTrialID        string `json:"powerTrialId,omitempty"`
	PublicationBoundary string `json:"publicationBoundary,omitempty"`
	Repetition          int    `json:"repetition,omitempty"`
}

type qualificationSessionEvent struct {
	SchemaVersion       uint32    `json:"schemaVersion"`
	SessionID           string    `json:"sessionId"`
	PlanSHA256          string    `json:"planSha256"`
	Ordinal             int       `json:"ordinal"`
	StepID              string    `json:"stepId"`
	Kind                string    `json:"kind"`
	ReceiptRelativePath string    `json:"receiptRelativePath"`
	ReceiptSHA256       string    `json:"receiptSha256"`
	PreviousEventSHA256 string    `json:"previousEventSha256,omitempty"`
	RecordedAt          time.Time `json:"recordedAt"`
}

type qualificationSessionState struct {
	Events          []qualificationSessionEvent
	EventRaw        [][]byte
	Durability      *durabilityCheckResult
	LastFinishedAt  time.Time
	ReceiptDigests  map[string]struct{}
	BootTransitions map[string]struct{}
}

var (
	qualificationSessionBuildIdentity    = durabilityBuildIdentity
	qualificationSessionValidateReceipt  = validateQualificationSessionReceipt
	qualificationSessionLoadReceiptState = loadQualificationSessionReceiptState
	qualificationSessionRuntimeIdentity  = func() (string, string, string) { return runtime.GOOS, runtime.GOARCH, runtime.Version() }
	qualificationSessionEffectiveUID     = os.Geteuid
)

func runQualificationSessionInit(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	rootPath := flags.String("artifacts-root", "", "secured campaign artifact root")
	environmentPath := flags.String("environment-record", "", "captured qualification environment inside the artifact root")
	sourceRevision := flags.String("source-revision", "", "exact 40- or 64-hex release revision")
	platformClass := flags.String("platform-class", "", "bounded public platform class")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *rootPath == "" || *environmentPath == "" || !validDurabilitySourceRevision(*sourceRevision) || !qualificationSafeName(*platformClass, 128) || flags.NArg() != 0 {
		return errors.New("qualification-session-init requires artifact root, environment record, source revision and platform class")
	}
	root, err := qualificationArtifactRoot(*rootPath)
	if err != nil {
		return err
	}
	environmentAbsolute, err := qualificationArtifactCandidate(*environmentPath)
	if err != nil || !qualificationPathWithin(root, environmentAbsolute) || environmentAbsolute == root {
		return errors.New("qualification session environment record must be a regular file inside the artifact root")
	}
	var environment qualificationEnvironmentEvidence
	environmentRaw, err := readQualificationReceipt(environmentAbsolute, &environment)
	if err != nil {
		return err
	}
	if err := validateQualificationEnvironmentEvidence(environment, *sourceRevision, ""); err != nil {
		return err
	}
	buildRevision, buildModified := qualificationSessionBuildIdentity()
	if buildRevision != *sourceRevision || buildModified {
		return errors.New("qualification session requires a clean binary built from the qualified revision")
	}
	goos, goarch, goVersion := qualificationSessionRuntimeIdentity()
	if goos != environment.GOOS || goarch != environment.GOARCH || goVersion != environment.GoVersion {
		return errors.New("qualification session command runtime differs from the captured environment")
	}
	if qualificationSessionEffectiveUID() != environment.HostOperator.EffectiveUID {
		return errors.New("qualification session command user differs from the captured operator")
	}
	operatorPath, err := qualificationArtifactCandidate(environment.HostOperator.OperatorEvidencePath)
	if err != nil || !qualificationPathWithin(root, operatorPath) || operatorPath == root {
		return errors.New("qualification session operator evidence must be inside the artifact root")
	}
	operatorSHA, err := hashRegularFile(operatorPath, qualificationReceiptMaxBytes)
	if err != nil || operatorSHA != environment.HostOperator.OperatorEvidenceSHA256 {
		return errors.New("qualification session operator evidence differs from the environment record")
	}
	executablePath, executableBytes, executableSHA256, err := qualificationSessionCurrentExecutable()
	if err != nil {
		return err
	}
	environmentRelative, err := filepath.Rel(root, environmentAbsolute)
	if err != nil || environmentRelative == "." || filepath.IsAbs(environmentRelative) || strings.HasPrefix(environmentRelative, ".."+string(filepath.Separator)) {
		return errors.New("qualification session environment path escapes the artifact root")
	}
	sessionIDRaw := make([]byte, 16)
	if _, err := rand.Read(sessionIDRaw); err != nil {
		return err
	}
	plan := qualificationSessionPlan{
		SchemaVersion: qualificationSessionPlanSchema, SessionID: hex.EncodeToString(sessionIDRaw),
		SourceRevision: *sourceRevision, BuildRevision: buildRevision, BuildModified: buildModified, CreatedAt: time.Now().UTC(),
		GOOS: environment.GOOS, GOARCH: environment.GOARCH, GoVersion: environment.GoVersion, PlatformClass: *platformClass,
		ArtifactsRoot: root, ExecutableRelativePath: qualificationSessionExecutable,
		ExecutableBytes: executableBytes, ExecutableSHA256: executableSHA256,
		EnvironmentRelativePath: filepath.ToSlash(environmentRelative), EnvironmentSHA256: qualificationSHA256(environmentRaw),
		Volume: environment.Volume, ControllerMethod: environment.Controller.Method, ControllerPublicKeySHA256: environment.Controller.AttestationPublicKeySHA256, ControllerTargetIdentitySHA256: environment.Controller.PowerTargetIdentitySHA256, Steps: qualificationSessionSteps(),
	}
	if err := validateQualificationSessionPlan(plan); err != nil {
		return err
	}
	sessionDirectory := filepath.Join(root, qualificationSessionDirectory)
	if err := os.Mkdir(sessionDirectory, 0o700); err != nil {
		return err
	}
	if err := syncProbeDirectory(root); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(sessionDirectory, "events"), 0o700); err != nil {
		return err
	}
	if err := syncProbeDirectory(sessionDirectory); err != nil {
		return err
	}
	retainedExecutablePath := filepath.Join(root, filepath.FromSlash(plan.ExecutableRelativePath))
	if err := copyFileExclusiveDurable(retainedExecutablePath, executablePath); err != nil {
		return err
	}
	retainedExecutableSHA256, err := hashRegularFile(retainedExecutablePath, int64(plan.ExecutableBytes))
	if err != nil || retainedExecutableSHA256 != plan.ExecutableSHA256 {
		return errors.New("qualification session executable changed while it was retained")
	}
	if err := writeJSONExclusiveDurable(filepath.Join(sessionDirectory, "plan.json"), plan); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(plan)
}

func runQualificationSessionRecord(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-record", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "qualification session plan")
	kind := flags.String("kind", "", "exact next step kind")
	receiptPath := flags.String("receipt", "", "existing receipt inside the artifact root")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *planPath == "" || *kind == "" || *receiptPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-session-record requires --plan, --kind and --receipt")
	}
	plan, planRaw, state, err := loadQualificationSession(*planPath, true)
	if err != nil {
		return err
	}
	if len(state.Events) >= len(plan.Steps) {
		return errors.New("qualification session evidence collection is already complete")
	}
	step := plan.Steps[len(state.Events)]
	if *kind != step.Kind {
		return fmt.Errorf("qualification session next step is %q, not %q", step.Kind, *kind)
	}
	receiptAbsolute, err := qualificationArtifactCandidate(*receiptPath)
	if err != nil || !qualificationPathWithin(plan.ArtifactsRoot, receiptAbsolute) || receiptAbsolute == plan.ArtifactsRoot {
		return errors.New("qualification session receipt must be a regular file inside the artifact root")
	}
	receiptRaw, finishedAt, err := qualificationSessionValidateReceipt(plan, step, receiptAbsolute, &state)
	if err != nil {
		return err
	}
	digest := qualificationSHA256(receiptRaw)
	if _, duplicate := state.ReceiptDigests[digest]; duplicate {
		return errors.New("qualification session repeats an exact receipt digest")
	}
	relative, err := filepath.Rel(plan.ArtifactsRoot, receiptAbsolute)
	if err != nil || relative == "." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("qualification session receipt path escapes the artifact root")
	}
	if relative == qualificationSessionDirectory || strings.HasPrefix(relative, qualificationSessionDirectory+string(filepath.Separator)) {
		return errors.New("qualification session receipt must not be stored in the session journal directory")
	}
	event := qualificationSessionEvent{
		SchemaVersion: qualificationSessionEventSchema, SessionID: plan.SessionID, PlanSHA256: qualificationSHA256(planRaw),
		Ordinal: step.Ordinal, StepID: step.ID, Kind: step.Kind, ReceiptRelativePath: filepath.ToSlash(relative), ReceiptSHA256: digest,
		RecordedAt: time.Now().UTC(),
	}
	if len(state.EventRaw) != 0 {
		event.PreviousEventSHA256 = qualificationSHA256(state.EventRaw[len(state.EventRaw)-1])
	}
	if !finishedAt.IsZero() && event.RecordedAt.Before(finishedAt) {
		return errors.New("qualification session event time precedes its receipt completion")
	}
	eventPath := filepath.Join(plan.ArtifactsRoot, qualificationSessionDirectory, "events", qualificationSessionEventFilename(step))
	if err := writeJSONExclusiveDurable(eventPath, event); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(event)
}

func runQualificationSessionStatus(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "qualification session plan")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *planPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-session-status requires --plan")
	}
	plan, _, state, err := loadQualificationSession(*planPath, false)
	if err != nil {
		return err
	}
	result := qualificationSessionStatusResult(plan, state)
	return json.NewEncoder(stdout).Encode(result)
}

func runQualificationSessionSeal(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-session-seal", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "qualification session plan")
	outputPath := flags.String("out", "", "new artifact index outside the artifact root")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *planPath == "" || *outputPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-session-seal requires --plan and --out")
	}
	plan, _, state, err := loadQualificationSession(*planPath, true)
	if err != nil {
		return err
	}
	if len(state.Events) != len(plan.Steps) {
		return fmt.Errorf("qualification session is incomplete: %d of %d steps recorded", len(state.Events), len(plan.Steps))
	}
	output, err := qualificationSessionExternalOutput(plan.ArtifactsRoot, *outputPath)
	if err != nil {
		return err
	}
	index, err := buildQualificationArtifactIndex(plan.ArtifactsRoot, plan.SourceRevision)
	if err != nil {
		return err
	}
	_, _, stateAfter, err := loadQualificationSession(*planPath, false)
	if err != nil || len(stateAfter.Events) != len(state.Events) {
		return errors.New("qualification session changed while sealing")
	}
	indexAfter, err := buildQualificationArtifactIndex(plan.ArtifactsRoot, plan.SourceRevision)
	if err != nil || !reflect.DeepEqual(index, indexAfter) {
		return errors.New("qualification artifact tree changed while sealing")
	}
	if err := writeJSONExclusiveDurable(output, index); err != nil {
		return err
	}
	indexSHA256, err := hashRegularFile(output, qualificationReceiptMaxBytes)
	if err != nil {
		return err
	}
	result := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		SessionID     string `json:"sessionId"`
		IndexSHA256   string `json:"indexSha256"`
		Entries       int    `json:"entries"`
		TotalBytes    uint64 `json:"totalBytes"`
		Sealed        bool   `json:"sealed"`
	}{1, plan.SessionID, indexSHA256, len(index.Entries), index.TotalBytes, true}
	return json.NewEncoder(stdout).Encode(result)
}

type qualificationSessionStatus struct {
	SchemaVersion uint32                    `json:"schemaVersion"`
	SessionID     string                    `json:"sessionId"`
	Completed     int                       `json:"completed"`
	Total         int                       `json:"total"`
	Next          *qualificationSessionStep `json:"next,omitempty"`
	ReadyToSeal   bool                      `json:"readyToSeal"`
}

func qualificationSessionStatusResult(plan qualificationSessionPlan, state qualificationSessionState) qualificationSessionStatus {
	result := qualificationSessionStatus{SchemaVersion: 1, SessionID: plan.SessionID, Completed: len(state.Events), Total: len(plan.Steps), ReadyToSeal: len(state.Events) == len(plan.Steps)}
	if !result.ReadyToSeal {
		next := plan.Steps[len(state.Events)]
		result.Next = &next
	}
	return result
}

func qualificationSessionSteps() []qualificationSessionStep {
	steps := []qualificationSessionStep{
		{ID: "durability", Kind: "durability"}, {ID: "soak", Kind: "soak"}, {ID: "process", Kind: "process"},
		{ID: "capacity", Kind: "capacity"}, {ID: "corruption", Kind: "corruption"},
	}
	for boundaryIndex, boundary := range qualificationPublicationBoundaries {
		for repetition := 1; repetition <= qualificationMinimumBoundaryTrials; repetition++ {
			id := fmt.Sprintf("power-%02d-%02d", boundaryIndex+1, repetition)
			steps = append(steps, qualificationSessionStep{ID: id, Kind: "power", PowerTrialID: id, PublicationBoundary: boundary, Repetition: repetition})
		}
	}
	for index := range steps {
		steps[index].Ordinal = index + 1
	}
	return steps
}

func validateQualificationSessionPlan(plan qualificationSessionPlan) error {
	if plan.SchemaVersion != qualificationSessionPlanSchema || !anchorQualificationHex(plan.SessionID, 16) ||
		!validDurabilitySourceRevision(plan.SourceRevision) || plan.BuildRevision != plan.SourceRevision || plan.BuildModified || plan.CreatedAt.IsZero() ||
		plan.GOOS != "linux" || plan.GOARCH == "" || plan.GoVersion == "" || !qualificationSafeName(plan.PlatformClass, 128) ||
		!filepath.IsAbs(plan.ArtifactsRoot) || plan.ExecutableRelativePath != qualificationSessionExecutable ||
		plan.ExecutableBytes == 0 || !qualificationHexDigest(plan.ExecutableSHA256) || validateQualificationArtifactPath(plan.EnvironmentRelativePath) != nil ||
		!qualificationHexDigest(plan.EnvironmentSHA256) || !qualificationPowerMethod(plan.ControllerMethod) ||
		plan.Volume.SchemaVersion != destructiveVolumeSchema || !plan.Volume.Eligible || plan.Volume.Device == 0 || plan.Volume.ControlDevice == 0 {
		return errors.New("qualification session plan identity, build, environment or volume is invalid")
	}
	if qualificationPhysicalPowerMethod(plan.ControllerMethod) != qualificationHexDigest(plan.ControllerPublicKeySHA256) || qualificationPhysicalPowerMethod(plan.ControllerMethod) != qualificationHexDigest(plan.ControllerTargetIdentitySHA256) {
		return errors.New("qualification session controller attestation key binding is missing or unexpected")
	}
	if !reflect.DeepEqual(plan.Steps, qualificationSessionSteps()) {
		return errors.New("qualification session plan does not contain the fixed evidence and power matrix")
	}
	return nil
}

func loadQualificationSession(planPath string, semantic bool) (qualificationSessionPlan, []byte, qualificationSessionState, error) {
	var plan qualificationSessionPlan
	planRaw, err := readQualificationReceipt(planPath, &plan)
	if err != nil {
		return plan, nil, qualificationSessionState{}, err
	}
	if err := validateQualificationSessionPlan(plan); err != nil {
		return plan, nil, qualificationSessionState{}, err
	}
	goos, goarch, goVersion := qualificationSessionRuntimeIdentity()
	if goos != plan.GOOS || goarch != plan.GOARCH || goVersion != plan.GoVersion {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session command runtime differs from its immutable plan")
	}
	_, currentExecutableBytes, currentExecutableSHA256, err := qualificationSessionCurrentExecutable()
	if err != nil || currentExecutableBytes != plan.ExecutableBytes || currentExecutableSHA256 != plan.ExecutableSHA256 {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session command executable differs from its immutable plan")
	}
	expectedPlanPath := filepath.Join(plan.ArtifactsRoot, qualificationSessionDirectory, "plan.json")
	actualPlanPath, err := qualificationArtifactCandidate(planPath)
	if err != nil || actualPlanPath != expectedPlanPath {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session plan path differs from its artifact root")
	}
	environmentPath := filepath.Join(plan.ArtifactsRoot, filepath.FromSlash(plan.EnvironmentRelativePath))
	retainedExecutablePath := filepath.Join(plan.ArtifactsRoot, filepath.FromSlash(plan.ExecutableRelativePath))
	retainedExecutableInfo, err := os.Lstat(retainedExecutablePath)
	if err != nil || retainedExecutableInfo.Mode()&os.ModeSymlink != 0 || !retainedExecutableInfo.Mode().IsRegular() || uint64(retainedExecutableInfo.Size()) != plan.ExecutableBytes {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session retained executable differs from its immutable plan")
	}
	retainedExecutableSHA256, err := hashRegularFile(retainedExecutablePath, int64(plan.ExecutableBytes))
	if err != nil || retainedExecutableSHA256 != plan.ExecutableSHA256 {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session retained executable differs from its immutable plan")
	}
	var environment qualificationEnvironmentEvidence
	environmentRaw, err := readQualificationReceipt(environmentPath, &environment)
	if err != nil || qualificationSHA256(environmentRaw) != plan.EnvironmentSHA256 || environment.Volume != plan.Volume || environment.Controller.Method != plan.ControllerMethod || environment.Controller.AttestationPublicKeySHA256 != plan.ControllerPublicKeySHA256 || environment.Controller.PowerTargetIdentitySHA256 != plan.ControllerTargetIdentitySHA256 {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session environment differs from its immutable plan")
	}
	if err := validateQualificationEnvironmentEvidence(environment, plan.SourceRevision, plan.ControllerMethod); err != nil {
		return plan, nil, qualificationSessionState{}, err
	}
	if qualificationSessionEffectiveUID() != environment.HostOperator.EffectiveUID {
		return plan, nil, qualificationSessionState{}, errors.New("qualification session command user differs from its immutable environment")
	}
	state := qualificationSessionState{ReceiptDigests: make(map[string]struct{}), BootTransitions: make(map[string]struct{})}
	eventsDirectory := filepath.Join(plan.ArtifactsRoot, qualificationSessionDirectory, "events")
	entries, err := os.ReadDir(eventsDirectory)
	if err != nil {
		return plan, nil, state, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	if len(entries) > len(plan.Steps) {
		return plan, nil, state, errors.New("qualification session journal contains too many events")
	}
	for index, entry := range entries {
		step := plan.Steps[index]
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || entry.Name() != qualificationSessionEventFilename(step) {
			return plan, nil, state, fmt.Errorf("qualification session journal entry %d is unexpected", index+1)
		}
		var event qualificationSessionEvent
		raw, err := readQualificationReceipt(filepath.Join(eventsDirectory, entry.Name()), &event)
		if err != nil {
			return plan, nil, state, err
		}
		previous := ""
		if index != 0 {
			previous = qualificationSHA256(state.EventRaw[index-1])
		}
		if event.SchemaVersion != qualificationSessionEventSchema || event.SessionID != plan.SessionID || event.PlanSHA256 != qualificationSHA256(planRaw) ||
			event.Ordinal != step.Ordinal || event.StepID != step.ID || event.Kind != step.Kind || event.PreviousEventSHA256 != previous ||
			event.RecordedAt.Before(plan.CreatedAt) || validateQualificationArtifactPath(event.ReceiptRelativePath) != nil || !qualificationHexDigest(event.ReceiptSHA256) {
			return plan, nil, state, fmt.Errorf("qualification session journal event %d is invalid", index+1)
		}
		if index != 0 && event.RecordedAt.Before(state.Events[index-1].RecordedAt) {
			return plan, nil, state, fmt.Errorf("qualification session journal event %d precedes its predecessor", index+1)
		}
		receiptRelative := filepath.FromSlash(event.ReceiptRelativePath)
		if receiptRelative == qualificationSessionDirectory || strings.HasPrefix(receiptRelative, qualificationSessionDirectory+string(filepath.Separator)) {
			return plan, nil, state, fmt.Errorf("qualification session receipt %d is stored in the journal directory", index+1)
		}
		receiptPath := filepath.Join(plan.ArtifactsRoot, receiptRelative)
		receiptSHA256, err := hashRegularFile(receiptPath, qualificationReceiptMaxBytes)
		if err != nil || receiptSHA256 != event.ReceiptSHA256 {
			return plan, nil, state, fmt.Errorf("qualification session receipt %d differs from its event", index+1)
		}
		if _, duplicate := state.ReceiptDigests[event.ReceiptSHA256]; duplicate {
			return plan, nil, state, fmt.Errorf("qualification session event %d repeats a receipt", index+1)
		}
		if semantic {
			_, finishedAt, err := qualificationSessionValidateReceipt(plan, step, receiptPath, &state)
			if err != nil {
				return plan, nil, state, fmt.Errorf("qualification session event %d: %w", index+1, err)
			}
			if !finishedAt.IsZero() && event.RecordedAt.Before(finishedAt) {
				return plan, nil, state, fmt.Errorf("qualification session event %d precedes its receipt completion", index+1)
			}
		} else if err := qualificationSessionLoadReceiptState(step, receiptPath, &state); err != nil {
			return plan, nil, state, fmt.Errorf("qualification session event %d: %w", index+1, err)
		}
		state.ReceiptDigests[event.ReceiptSHA256] = struct{}{}
		state.Events = append(state.Events, event)
		state.EventRaw = append(state.EventRaw, raw)
	}
	return plan, planRaw, state, nil
}

func qualificationSessionEventFilename(step qualificationSessionStep) string {
	return fmt.Sprintf("%03d-%s.json", step.Ordinal, step.ID)
}

func qualificationSessionExternalOutput(root, path string) (string, error) {
	output, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return "", err
	}
	output = filepath.Join(parent, filepath.Base(output))
	if qualificationPathWithin(root, output) {
		return "", errors.New("qualification session seal output must be outside the artifact root")
	}
	return output, nil
}

func qualificationSessionCurrentExecutable() (string, uint64, string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", 0, "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", 0, "", err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return "", 0, "", errors.New("qualification session executable is not a nonempty regular file")
	}
	digest, err := hashRegularFile(path, info.Size())
	if err != nil {
		return "", 0, "", err
	}
	return path, uint64(info.Size()), digest, nil
}
