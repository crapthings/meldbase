package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/crapthings/meldbase/core"
	"github.com/crapthings/meldbase/integrations/anchorhttp"
)

const anchorQualificationSchema uint32 = 1

var anchorQualificationPhases = [...]string{"healthy", "degraded", "minority", "recovered", "rollback-rejected"}

type anchorQualificationMember struct {
	MemberID              string `json:"memberId"`
	EndpointSHA256        string `json:"endpointSha256"`
	State                 string `json:"state"`
	Exists                bool   `json:"exists"`
	DatabaseIDHex         string `json:"databaseIdHex,omitempty"`
	MinimumCommitSequence uint64 `json:"minimumCommitSequence"`
	MinimumGeneration     uint64 `json:"minimumGeneration"`
}

type anchorQualificationReceipt struct {
	SchemaVersion          uint32                      `json:"schemaVersion"`
	ProtocolVersion        uint32                      `json:"protocolVersion"`
	Phase                  string                      `json:"phase"`
	RunID                  string                      `json:"runId"`
	PreviousSHA256         string                      `json:"previousSha256,omitempty"`
	SourceRevision         string                      `json:"sourceRevision,omitempty"`
	BuildRevision          string                      `json:"buildRevision,omitempty"`
	BuildModified          bool                        `json:"buildModified"`
	GOOS                   string                      `json:"goos"`
	GOARCH                 string                      `json:"goarch"`
	GoVersion              string                      `json:"goVersion"`
	StartedAt              time.Time                   `json:"startedAt"`
	FinishedAt             time.Time                   `json:"finishedAt"`
	ConfigurationID        string                      `json:"configurationId"`
	ExternalEvidenceSHA256 string                      `json:"externalEvidenceSha256"`
	Replicas               uint64                      `json:"replicas"`
	Quorum                 uint64                      `json:"quorum"`
	Members                []anchorQualificationMember `json:"members"`
	AvailableMembers       uint64                      `json:"availableMembers"`
	UnavailableMembers     uint64                      `json:"unavailableMembers"`
	QuorumLoad             string                      `json:"quorumLoad"`
	AnchorExists           bool                        `json:"anchorExists"`
	AnchorSequence         uint64                      `json:"anchorSequence"`
	AnchorGeneration       uint64                      `json:"anchorGeneration"`
	Database               meldbase.VerificationReport `json:"database"`
	DatabaseRelation       string                      `json:"databaseRelation"`
	DatabaseOpen           string                      `json:"databaseOpen"`
	Passed                 bool                        `json:"passed"`
	SigningPublicKey       string                      `json:"signingPublicKey"`
	Signature              string                      `json:"signature"`
}

func runAnchorQualification(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("anchor-qualification requires keygen, probe, verify, history-agent, history-run, history-sign or history-verify")
	}
	switch args[0] {
	case "keygen":
		return runAnchorQualificationKeygen(args[1:], stdout, stderr)
	case "probe":
		return runAnchorQualificationProbe(args[1:], stdout, stderr)
	case "verify":
		return runAnchorQualificationVerify(args[1:], stdout, stderr)
	case "history-sign":
		return runAnchorHistoryQualificationSign(args[1:], stdout, stderr)
	case "history-verify":
		return runAnchorHistoryQualificationVerify(args[1:], stdout, stderr)
	case "history-agent":
		return runAnchorHistoryAgent(args[1:], stdout, stderr)
	case "history-run":
		return runAnchorHistoryQualificationRun(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown anchor-qualification action %q", args[0])
	}
}

func runAnchorQualificationKeygen(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification keygen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	privatePath := flags.String("private", "", "new private Ed25519 signing-key file")
	publicPath := flags.String("public", "", "new public Ed25519 verification-key file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *privatePath == "" || *publicPath == "" || flags.NArg() != 0 {
		return errors.New("anchor-qualification keygen requires --private and --public")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := writeAnchorQualificationKey(*privatePath, privateKey, 0o600); err != nil {
		return err
	}
	if err := writeAnchorQualificationKey(*publicPath, publicKey, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Created anchor qualification signing key and verification key\n")
	return nil
}

func runAnchorQualificationProbe(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification probe", flag.ContinueOnError)
	flags.SetOutput(stderr)
	phase := flags.String("phase", "", "healthy, degraded, minority, recovered or rollback-rejected")
	databasePath := flags.String("db", "", "offline disposable database path")
	previousPath := flags.String("previous", "", "previous signed phase receipt")
	outputPath := flags.String("out", "", "new exclusive signed phase receipt")
	signingKeyPath := flags.String("signing-key", "", "private Ed25519 receipt signing key")
	sourceRevision := flags.String("source-revision", "", "optional exact 40- or 64-hex source revision")
	requireClean := flags.Bool("require-clean-source", false, "require clean build metadata matching source revision")
	externalEvidence := flags.String("external-evidence-sha256", "", "SHA-256 of secured topology/controller evidence for this phase")
	timeout := flags.Duration("timeout", 10*time.Second, "deadline for each full membership or quorum operation")
	verificationTimeout := flags.Duration("verification-timeout", 5*time.Minute, "deadline for each offline full database verification")
	cluster := flags.String("cluster", "", "static anchor cluster ID")
	anchorName := flags.String("anchor-name", "", "anchor resource name")
	keyID := flags.String("key-id", "", "anchor HMAC key ID")
	keyFile := flags.String("key-file", "", "private base64 anchor HMAC key file")
	serverCA := flags.String("ca", "", "anchor server CA PEM")
	clientCert := flags.String("client-cert", "", "anchor mTLS client certificate PEM")
	clientKey := flags.String("client-key", "", "private anchor mTLS client key PEM")
	var replicas anchorReplicaFlags
	flags.Var(&replicas, "replica", "repeatable member-id=https://endpoint")
	if err := flags.Parse(args); err != nil {
		return err
	}
	phaseIndex := anchorQualificationPhaseIndex(*phase)
	if phaseIndex < 0 || *databasePath == "" || *outputPath == "" || *signingKeyPath == "" || !qualificationHexDigest(*externalEvidence) || *timeout <= 0 || *verificationTimeout <= 0 || *verificationTimeout > time.Hour || flags.NArg() != 0 {
		return errors.New("anchor-qualification probe requires a valid phase, db, out, signing key and positive timeout")
	}
	if (phaseIndex == 0) != (*previousPath == "") {
		return errors.New("healthy must not have --previous; every later phase requires it")
	}
	if *sourceRevision != "" && !validDurabilitySourceRevision(*sourceRevision) {
		return errors.New("invalid anchor qualification source revision")
	}
	buildRevision, buildModified := durabilityBuildIdentity()
	if *requireClean && (*sourceRevision == "" || buildRevision != *sourceRevision || buildModified) {
		return errors.New("anchor qualification clean source verification failed")
	}
	privateKey, err := loadAnchorQualificationPrivateKey(*signingKeyPath)
	if err != nil {
		return err
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var previous anchorQualificationReceipt
	var previousRaw []byte
	if phaseIndex > 0 {
		previousRaw, err = readQualificationReceipt(*previousPath, &previous)
		if err != nil {
			return fmt.Errorf("previous anchor qualification receipt: %w", err)
		}
		if err := verifyAnchorQualificationReceipt(previous, publicKey); err != nil {
			return fmt.Errorf("previous anchor qualification receipt: %w", err)
		}
		if previous.Phase != anchorQualificationPhases[phaseIndex-1] {
			return errors.New("previous anchor qualification phase is not the immediate predecessor")
		}
	}
	config := remoteAnchorConfig{
		clusterID: *cluster, replicaSpecs: replicas, anchorName: *anchorName, keyID: *keyID, keyFile: *keyFile,
		serverCAFile: *serverCA, clientCertFile: *clientCert, clientKeyFile: *clientKey, operationTimeout: *timeout,
	}
	store, transport, err := newRemoteAnchorStore(config)
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	started := time.Now().UTC()
	verificationContext, verificationCancel := context.WithTimeout(context.Background(), *verificationTimeout)
	verified, err := meldbase.VerifyFile(verificationContext, *databasePath)
	verificationCancel()
	if err != nil {
		return fmt.Errorf("offline qualification database verification: %w", err)
	}
	if !verified.Verified || !verified.IndexContentsVerified || !verified.IndexBuildContentsVerified {
		return errors.New("offline qualification database verification is semantically incomplete")
	}
	operation, cancel := context.WithTimeout(context.Background(), *timeout)
	checks, err := store.CheckReplicas(operation)
	cancel()
	if err != nil {
		return err
	}
	members, available, unavailable := anchorQualificationMembers(checks, config.replicaSpecs)
	if phaseIndex > 0 && !sameAnchorQualificationMembers(previous.Members, members) {
		return errors.New("anchor qualification phase changed its member-to-endpoint mapping")
	}
	operation, cancel = context.WithTimeout(context.Background(), *timeout)
	anchor, anchorExists, loadErr := store.Load(operation)
	cancel()
	quorumLoad := "succeeded"
	if loadErr != nil {
		if !errors.Is(loadErr, anchorhttp.ErrQuorum) {
			return fmt.Errorf("anchor quorum load: %w", loadErr)
		}
		quorumLoad = "unavailable"
	}
	relation, err := anchorQualificationRelation(verified, anchor, anchorExists, loadErr)
	if err != nil {
		return err
	}
	databaseOpen := "not-attempted"
	if *phase == "recovered" || *phase == "rollback-rejected" {
		opened, openErr := meldbase.OpenWithOptions(*databasePath, meldbase.OpenOptions{RollbackProtection: meldbase.RollbackProtection{AnchorStore: store, OperationTimeout: *timeout}})
		if *phase == "rollback-rejected" {
			if !errors.Is(openErr, meldbase.ErrRollbackDetected) || opened != nil {
				if opened != nil {
					_ = opened.Close()
				}
				return fmt.Errorf("rollback phase did not reject database: %w", openErr)
			}
			databaseOpen = "rollback-rejected"
		} else {
			if openErr != nil || opened == nil {
				return fmt.Errorf("recovered phase database open: %w", openErr)
			}
			if err := opened.Close(); err != nil {
				return err
			}
			databaseOpen = "succeeded"
			verificationContext, verificationCancel = context.WithTimeout(context.Background(), *verificationTimeout)
			verified, err = meldbase.VerifyFile(verificationContext, *databasePath)
			verificationCancel()
			if err != nil {
				return err
			}
		}
	}
	quorum := uint64(len(config.replicaSpecs)/2 + 1)
	if err := validateAnchorQualificationPhase(*phase, uint64(len(config.replicaSpecs)), quorum, available, unavailable, quorumLoad, relation, databaseOpen, anchorExists); err != nil {
		return err
	}
	runID := ""
	if phaseIndex == 0 {
		runID, err = newAnchorQualificationRunID()
		if err != nil {
			return err
		}
	} else {
		runID = previous.RunID
		if previous.ConfigurationID != store.ConfigurationID() || previous.Database.DatabaseIDHex != verified.DatabaseIDHex || previous.SourceRevision != *sourceRevision ||
			previous.BuildRevision != buildRevision || previous.BuildModified != buildModified || previous.GOOS != runtime.GOOS || previous.GOARCH != runtime.GOARCH || previous.GoVersion != runtime.Version() {
			return errors.New("anchor qualification phase changed configuration, database identity, build or source revision")
		}
	}
	receipt := anchorQualificationReceipt{
		SchemaVersion: anchorQualificationSchema, ProtocolVersion: anchorhttp.ProtocolVersion, Phase: *phase, RunID: runID,
		SourceRevision: *sourceRevision, BuildRevision: buildRevision, BuildModified: buildModified, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		StartedAt: started, FinishedAt: time.Now().UTC(), ConfigurationID: store.ConfigurationID(), Replicas: uint64(len(config.replicaSpecs)), Quorum: quorum,
		ExternalEvidenceSHA256: *externalEvidence,
		Members:                members, AvailableMembers: available, UnavailableMembers: unavailable, QuorumLoad: quorumLoad,
		AnchorExists: anchorExists, AnchorSequence: anchor.MinimumCommitSequence, AnchorGeneration: anchor.MinimumGeneration,
		Database: verified, DatabaseRelation: relation, DatabaseOpen: databaseOpen, Passed: true,
		SigningPublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}
	if len(previousRaw) > 0 {
		receipt.PreviousSHA256 = qualificationSHA256(previousRaw)
	}
	if err := signAnchorQualificationReceipt(&receipt, privateKey); err != nil {
		return err
	}
	if err := validateAnchorQualificationReceipt(receipt); err != nil {
		return err
	}
	if err := writeJSONExclusiveDurable(*outputPath, receipt); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(receipt)
}

type anchorReceiptFlags []string

func (values *anchorReceiptFlags) String() string { return strings.Join(*values, ",") }
func (values *anchorReceiptFlags) Set(value string) error {
	if value == "" {
		return errors.New("empty anchor qualification receipt")
	}
	*values = append(*values, value)
	return nil
}

func runAnchorQualificationVerify(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("anchor-qualification verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	publicKeyPath := flags.String("public-key", "", "Ed25519 verification key")
	requireComplete := flags.Bool("require-complete", false, "require the complete five-phase chain")
	var paths anchorReceiptFlags
	flags.Var(&paths, "receipt", "phase receipt in chronological order; repeat")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *publicKeyPath == "" || len(paths) == 0 || flags.NArg() != 0 || (*requireComplete && len(paths) != len(anchorQualificationPhases)) {
		return errors.New("anchor-qualification verify requires public key and ordered receipts; complete mode requires five")
	}
	publicKey, err := loadAnchorQualificationPublicKey(*publicKeyPath)
	if err != nil {
		return err
	}
	verified, err := verifyAnchorQualificationChain(paths, publicKey, *requireComplete)
	if err != nil {
		return err
	}
	result := struct {
		SchemaVersion uint32 `json:"schemaVersion"`
		RunID         string `json:"runId"`
		Receipts      int    `json:"receipts"`
		Complete      bool   `json:"complete"`
		FinalSHA256   string `json:"finalSha256"`
		Passed        bool   `json:"passed"`
	}{1, verified.RunID, len(paths), verified.Complete, verified.ReceiptSHA256[len(verified.ReceiptSHA256)-1], true}
	return json.NewEncoder(stdout).Encode(result)
}

type anchorQualificationChainVerification struct {
	RunID           string
	ConfigurationID string
	Complete        bool
	Receipts        []anchorQualificationReceipt
	ReceiptSHA256   []string
}

func verifyAnchorQualificationChain(paths []string, publicKey ed25519.PublicKey, requireComplete bool) (anchorQualificationChainVerification, error) {
	if len(publicKey) != ed25519.PublicKeySize || len(paths) == 0 || (requireComplete && len(paths) != len(anchorQualificationPhases)) {
		return anchorQualificationChainVerification{}, errors.New("anchor qualification chain inputs are incomplete")
	}
	var previous anchorQualificationReceipt
	var previousRaw []byte
	seenExternalEvidence := make(map[string]struct{}, len(paths))
	verified := anchorQualificationChainVerification{
		Receipts: make([]anchorQualificationReceipt, 0, len(paths)), ReceiptSHA256: make([]string, 0, len(paths)),
	}
	for index, path := range paths {
		var receipt anchorQualificationReceipt
		raw, err := readQualificationReceipt(path, &receipt)
		if err != nil {
			return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d: %w", index+1, err)
		}
		if err := verifyAnchorQualificationReceipt(receipt, publicKey); err != nil {
			return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d: %w", index+1, err)
		}
		if _, duplicate := seenExternalEvidence[receipt.ExternalEvidenceSHA256]; duplicate {
			return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d reuses external controller evidence", index+1)
		}
		seenExternalEvidence[receipt.ExternalEvidenceSHA256] = struct{}{}
		if index >= len(anchorQualificationPhases) || receipt.Phase != anchorQualificationPhases[index] || (index == 0 && receipt.PreviousSHA256 != "") {
			return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d is out of phase order", index+1)
		}
		if index > 0 {
			if receipt.PreviousSHA256 != qualificationSHA256(previousRaw) || receipt.RunID != previous.RunID || receipt.ConfigurationID != previous.ConfigurationID ||
				receipt.Database.DatabaseIDHex != previous.Database.DatabaseIDHex || receipt.SourceRevision != previous.SourceRevision || receipt.BuildRevision != previous.BuildRevision || receipt.BuildModified != previous.BuildModified ||
				receipt.GOOS != previous.GOOS || receipt.GOARCH != previous.GOARCH || receipt.GoVersion != previous.GoVersion {
				return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d breaks its signed phase chain", index+1)
			}
			if receipt.StartedAt.Before(previous.FinishedAt) || !sameAnchorQualificationMembers(previous.Members, receipt.Members) {
				return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d changes timeline or member endpoint mapping", index+1)
			}
			if receipt.Database.CommitSequence < previous.Database.CommitSequence && receipt.Phase != "rollback-rejected" {
				return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d regresses database sequence before rollback phase", index+1)
			}
			if receipt.Phase == "rollback-rejected" && receipt.Database.CommitSequence >= previous.Database.CommitSequence {
				return anchorQualificationChainVerification{}, fmt.Errorf("receipt %d does not contain an older database image", index+1)
			}
		}
		verified.Receipts = append(verified.Receipts, receipt)
		verified.ReceiptSHA256 = append(verified.ReceiptSHA256, qualificationSHA256(raw))
		previous, previousRaw = receipt, raw
	}
	if requireComplete && (previous.Phase != "rollback-rejected" || previous.DatabaseOpen != "rollback-rejected") {
		return anchorQualificationChainVerification{}, errors.New("complete anchor qualification chain lacks final rollback rejection")
	}
	verified.RunID = previous.RunID
	verified.ConfigurationID = previous.ConfigurationID
	verified.Complete = len(paths) == len(anchorQualificationPhases)
	return verified, nil
}

func anchorQualificationMembers(checks []anchorhttp.ReplicaCheck, replicaSpecs []string) ([]anchorQualificationMember, uint64, uint64) {
	members := make([]anchorQualificationMember, len(checks))
	var available, unavailable uint64
	for index, check := range checks {
		endpoint := ""
		if _, value, ok := strings.Cut(replicaSpecs[index], "="); ok {
			endpoint = value
		}
		member := anchorQualificationMember{MemberID: check.MemberID, EndpointSHA256: bytesSHA256([]byte(endpoint)), State: string(check.State), Exists: check.Exists}
		if check.Exists {
			member.DatabaseIDHex = hex.EncodeToString(check.Anchor.DatabaseID[:])
			member.MinimumCommitSequence = check.Anchor.MinimumCommitSequence
			member.MinimumGeneration = check.Anchor.MinimumGeneration
		}
		members[index] = member
		if check.State == anchorhttp.ReplicaAvailable {
			available++
		} else if check.State == anchorhttp.ReplicaUnavailable {
			unavailable++
		}
	}
	return members, available, unavailable
}

func anchorQualificationRelation(database meldbase.VerificationReport, anchor meldbase.RollbackAnchor, exists bool, loadErr error) (string, error) {
	if loadErr != nil {
		return "unavailable", nil
	}
	if !exists {
		return "missing", errors.New("qualification quorum is missing its anchor")
	}
	if hex.EncodeToString(anchor.DatabaseID[:]) != database.DatabaseIDHex {
		return "identity-conflict", errors.New("qualification database and anchor identities differ")
	}
	if anchor.MinimumCommitSequence > database.CommitSequence || anchor.MinimumGeneration > database.MetaGeneration {
		return "anchor-ahead", nil
	}
	if anchor.MinimumCommitSequence == database.CommitSequence && anchor.MinimumGeneration == database.MetaGeneration {
		return "equal", nil
	}
	return "database-ahead", nil
}

func validateAnchorQualificationPhase(phase string, replicas, quorum, available, unavailable uint64, load, relation, opened string, anchorExists bool) error {
	switch phase {
	case "healthy":
		if available != replicas || load != "succeeded" || !anchorExists || relation != "equal" || opened != "not-attempted" {
			return errors.New("healthy phase requires every member and a successful quorum load")
		}
	case "degraded":
		if available != quorum || unavailable != replicas-quorum || load != "succeeded" || !anchorExists || relation != "equal" || opened != "not-attempted" {
			return errors.New("degraded phase requires exactly one maximum tolerated crash-fault set and a quorum")
		}
	case "minority":
		if available >= quorum || unavailable <= replicas-quorum || load != "unavailable" || relation != "unavailable" || opened != "not-attempted" || anchorExists {
			return errors.New("minority phase requires loss of quorum from unavailable members")
		}
	case "recovered":
		if available != replicas || load != "succeeded" || !anchorExists || (relation != "equal" && relation != "database-ahead") || opened != "succeeded" {
			return errors.New("recovered phase requires every member, quorum load and database reopen")
		}
	case "rollback-rejected":
		if available != replicas || load != "succeeded" || !anchorExists || relation != "anchor-ahead" || opened != "rollback-rejected" {
			return errors.New("rollback phase requires every member and explicit rollback rejection")
		}
	default:
		return errors.New("unknown anchor qualification phase")
	}
	return nil
}

func anchorQualificationPhaseIndex(phase string) int {
	for index, expected := range anchorQualificationPhases {
		if phase == expected {
			return index
		}
	}
	return -1
}

func validateAnchorQualificationReceipt(receipt anchorQualificationReceipt) error {
	if receipt.SchemaVersion != anchorQualificationSchema || receipt.ProtocolVersion != anchorhttp.ProtocolVersion || anchorQualificationPhaseIndex(receipt.Phase) < 0 ||
		!anchorQualificationHex(receipt.RunID, 16) || !qualificationHexDigest(receipt.ConfigurationID) || !qualificationHexDigest(receipt.ExternalEvidenceSHA256) || receipt.Replicas == 0 || receipt.Quorum != receipt.Replicas/2+1 ||
		uint64(len(receipt.Members)) != receipt.Replicas || receipt.StartedAt.IsZero() || !receipt.FinishedAt.After(receipt.StartedAt) || !receipt.Passed ||
		!receipt.Database.Verified || !anchorQualificationHex(receipt.Database.DatabaseIDHex, 16) || !qualificationHexDigest(receipt.Database.SHA256) || receipt.SigningPublicKey == "" || receipt.Signature == "" {
		return errors.New("anchor qualification receipt is incomplete")
	}
	if receipt.PreviousSHA256 != "" && !qualificationHexDigest(receipt.PreviousSHA256) {
		return errors.New("anchor qualification previous receipt digest is invalid")
	}
	if receipt.AnchorExists {
		if receipt.AnchorGeneration <= receipt.AnchorSequence {
			return errors.New("anchor qualification quorum anchor is invalid")
		}
	} else if receipt.AnchorSequence != 0 || receipt.AnchorGeneration != 0 {
		return errors.New("anchor qualification missing quorum anchor has coordinates")
	}
	seen := make(map[string]struct{}, len(receipt.Members))
	var available, unavailable uint64
	for _, member := range receipt.Members {
		if member.MemberID == "" || !qualificationHexDigest(member.EndpointSHA256) || (member.Exists && !anchorQualificationHex(member.DatabaseIDHex, 16)) {
			return errors.New("anchor qualification member evidence is invalid")
		}
		if _, duplicate := seen[member.MemberID]; duplicate {
			return errors.New("anchor qualification member evidence is duplicated")
		}
		seen[member.MemberID] = struct{}{}
		switch member.State {
		case string(anchorhttp.ReplicaAvailable):
			if !member.Exists || member.MinimumGeneration == 0 {
				return errors.New("available anchor qualification member lacks a valid anchor")
			}
			if member.DatabaseIDHex != receipt.Database.DatabaseIDHex {
				return errors.New("anchor qualification member has another database identity")
			}
			if receipt.Phase != "rollback-rejected" && (member.MinimumCommitSequence > receipt.Database.CommitSequence || member.MinimumGeneration > receipt.Database.MetaGeneration) {
				return errors.New("anchor qualification member is ahead of the database before rollback phase")
			}
			available++
		case string(anchorhttp.ReplicaUnavailable):
			if member.Exists {
				return errors.New("unavailable anchor qualification member claims an anchor")
			}
			unavailable++
		case string(anchorhttp.ReplicaMissing), string(anchorhttp.ReplicaAuthentication), string(anchorhttp.ReplicaConfiguration), string(anchorhttp.ReplicaProtocol):
			if member.Exists {
				return errors.New("failed anchor qualification member claims an anchor")
			}
		default:
			return errors.New("anchor qualification member state is invalid")
		}
	}
	if available != receipt.AvailableMembers || unavailable != receipt.UnavailableMembers {
		return errors.New("anchor qualification member counts do not match evidence")
	}
	if err := validateAnchorQualificationPhase(receipt.Phase, receipt.Replicas, receipt.Quorum, available, unavailable, receipt.QuorumLoad, receipt.DatabaseRelation, receipt.DatabaseOpen, receipt.AnchorExists); err != nil {
		return err
	}
	return nil
}

func anchorQualificationHex(value string, bytes int) bool {
	if len(value) != bytes*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func sameAnchorQualificationMembers(left, right []anchorQualificationMember) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].MemberID != right[index].MemberID || left[index].EndpointSHA256 != right[index].EndpointSHA256 {
			return false
		}
	}
	return true
}

func signAnchorQualificationReceipt(receipt *anchorQualificationReceipt, privateKey ed25519.PrivateKey) error {
	receipt.Signature = ""
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	receipt.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func verifyAnchorQualificationReceipt(receipt anchorQualificationReceipt, publicKey ed25519.PublicKey) error {
	if err := validateAnchorQualificationReceipt(receipt); err != nil {
		return err
	}
	embedded, err := base64.StdEncoding.Strict().DecodeString(receipt.SigningPublicKey)
	if err != nil || !ed25519.PublicKey(embedded).Equal(publicKey) {
		return errors.New("anchor qualification signing public key differs")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(receipt.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("anchor qualification signature is malformed")
	}
	receipt.Signature = ""
	payload, err := json.Marshal(receipt)
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("anchor qualification signature verification failed")
	}
	return nil
}

func newAnchorQualificationRunID() (string, error) {
	var identifier [16]byte
	if _, err := io.ReadFull(rand.Reader, identifier[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(identifier[:]), nil
}

func writeAnchorQualificationKey(path string, key []byte, mode os.FileMode) error {
	raw := []byte(base64.StdEncoding.EncodeToString(key) + "\n")
	clean, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	file, err := os.OpenFile(clean, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	writeErr = errors.Join(writeErr, file.Sync(), file.Close())
	return errors.Join(writeErr, syncProbeDirectory(filepath.Dir(clean)))
}

func loadAnchorQualificationPrivateKey(path string) (ed25519.PrivateKey, error) {
	raw, err := readAnchorFile(path, 1024, true)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeAnchorQualificationKey(raw, ed25519.PrivateKeySize)
	return ed25519.PrivateKey(decoded), err
}

func loadAnchorQualificationPublicKey(path string) (ed25519.PublicKey, error) {
	raw, err := readAnchorFile(path, 1024, false)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeAnchorQualificationKey(raw, ed25519.PublicKeySize)
	return ed25519.PublicKey(decoded), err
}

func decodeAnchorQualificationKey(raw []byte, size int) ([]byte, error) {
	encoded := strings.TrimSuffix(string(raw), "\n")
	if encoded == "" || strings.ContainsAny(encoded, "\r\n\t ") {
		return nil, errors.New("anchor qualification key must contain one strict base64 value")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != size {
		return nil, errors.New("anchor qualification key has the wrong type or size")
	}
	return decoded, nil
}
