package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"time"
)

const qualificationReleasePacketSchema uint32 = 1

var qualificationPacketBuildIdentity = durabilityBuildIdentity

type qualificationReleasePacket struct {
	SchemaVersion           uint32                       `json:"schemaVersion"`
	Qualification           qualificationCheckResult     `json:"qualification"`
	Verifier                qualificationReleaseVerifier `json:"verifier"`
	SignedAt                time.Time                    `json:"signedAt"`
	ReleaseSigningPublicKey string                       `json:"releaseSigningPublicKey"`
	Signature               string                       `json:"signature"`
}

type qualificationReleaseVerifier struct {
	BuildRevision string `json:"buildRevision"`
	BuildModified bool   `json:"buildModified"`
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	GoVersion     string `json:"goVersion"`
}

func runQualificationPacketKeygen(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-packet-keygen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	privatePath := flags.String("private", "", "new private Ed25519 release signing key")
	publicPath := flags.String("public", "", "new public Ed25519 release verification key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *privatePath == "" || *publicPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-packet-keygen requires --private and --public")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := writeAnchorQualificationKey(*privatePath, privateKey, 0o600); err != nil {
		return err
	}
	if err := writeAnchorQualificationKey(*publicPath, publicKey, 0o644); err != nil {
		if removeErr := os.Remove(*privatePath); removeErr != nil {
			return fmt.Errorf("write public key: %w; remove incomplete private key: %v", err, removeErr)
		}
		return err
	}
	_, err = fmt.Fprintln(stdout, "Created independent release packet signing and verification keys")
	return err
}

func newQualificationReleasePacket(result qualificationCheckResult, privateKeyPath string) (qualificationReleasePacket, error) {
	if err := validateQualificationReleaseResult(result); err != nil {
		return qualificationReleasePacket{}, err
	}
	buildRevision, buildModified := qualificationPacketBuildIdentity()
	if buildRevision != result.SourceRevision || buildModified {
		return qualificationReleasePacket{}, errors.New("signed release packet requires a clean verifier binary built from the qualified revision")
	}
	privateKey, err := loadAnchorQualificationPrivateKey(privateKeyPath)
	if err != nil {
		return qualificationReleasePacket{}, err
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if qualificationSHA256(publicKey) == result.AnchorPublicKeySHA256 {
		return qualificationReleasePacket{}, errors.New("release packet key must be independent from the rollback-anchor qualification key")
	}
	packet := qualificationReleasePacket{
		SchemaVersion: qualificationReleasePacketSchema, Qualification: result,
		Verifier: qualificationReleaseVerifier{
			BuildRevision: buildRevision, BuildModified: buildModified,
			GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		},
		SignedAt:                time.Now().UTC(),
		ReleaseSigningPublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}
	if err := signQualificationReleasePacket(&packet, privateKey); err != nil {
		return qualificationReleasePacket{}, err
	}
	if err := validateQualificationReleasePacket(packet); err != nil {
		return qualificationReleasePacket{}, err
	}
	return packet, nil
}

func signQualificationReleasePacket(packet *qualificationReleasePacket, privateKey ed25519.PrivateKey) error {
	packet.Signature = ""
	payload, err := json.Marshal(packet)
	if err != nil {
		return err
	}
	packet.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return nil
}

func verifyQualificationReleasePacket(packet qualificationReleasePacket, publicKey ed25519.PublicKey) error {
	if err := validateQualificationReleasePacket(packet); err != nil {
		return err
	}
	embedded, err := base64.StdEncoding.Strict().DecodeString(packet.ReleaseSigningPublicKey)
	if err != nil || !ed25519.PublicKey(embedded).Equal(publicKey) {
		return errors.New("release packet signing public key differs")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(packet.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("release packet signature is malformed")
	}
	packet.Signature = ""
	payload, err := json.Marshal(packet)
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("release packet signature verification failed")
	}
	return nil
}

func validateQualificationReleasePacket(packet qualificationReleasePacket) error {
	if packet.SchemaVersion != qualificationReleasePacketSchema || packet.Verifier.BuildRevision != packet.Qualification.SourceRevision || packet.Verifier.BuildModified ||
		packet.Verifier.GOOS == "" || packet.Verifier.GOARCH == "" || packet.Verifier.GoVersion == "" || packet.SignedAt.IsZero() ||
		packet.ReleaseSigningPublicKey == "" || packet.Signature == "" {
		return errors.New("release packet identity or verifier provenance is incomplete")
	}
	if err := validateQualificationReleaseResult(packet.Qualification); err != nil {
		return err
	}
	publicKey, err := base64.StdEncoding.Strict().DecodeString(packet.ReleaseSigningPublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize || qualificationSHA256(publicKey) == packet.Qualification.AnchorPublicKeySHA256 {
		return errors.New("release packet signing key is malformed or not independent")
	}
	return nil
}

func validateQualificationReleaseResult(result qualificationCheckResult) error {
	if result.SchemaVersion != 2 || !validDurabilitySourceRevision(result.SourceRevision) || result.EvidenceLevel != 5 ||
		!result.StorageQualified || !result.RollbackProtectionQualified || !result.ProductionQualified || !result.Passed ||
		result.GOOS == "" || result.GOARCH == "" || result.Device == 0 || result.FilesystemType == "" || result.FilesystemName == "" || result.BlockSize == 0 ||
		!qualificationHexDigest(result.DurabilityReceiptSHA256) || !qualificationHexDigest(result.SoakReceiptSHA256) || !qualificationHexDigest(result.DestructiveRecordSHA256) ||
		!qualificationHexDigest(result.AnchorPublicKeySHA256) || !qualificationHexDigest(result.AnchorHistoryReceiptSHA256) || !qualificationHexDigest(result.AnchorHistoryControllerSHA256) ||
		!anchorQualificationHex(result.AnchorRunID, 16) || !anchorQualificationHex(result.AnchorHistoryRunID, 16) || result.AnchorRunID == result.AnchorHistoryRunID ||
		!qualificationHexDigest(result.AnchorConfigurationID) || !qualificationHexDigest(result.AnchorHistoryConfigurationID) || result.AnchorConfigurationID == result.AnchorHistoryConfigurationID ||
		len(result.AnchorPhaseReceiptSHA256) != len(anchorQualificationPhases) {
		return errors.New("release packet does not contain a complete Level 5 qualification")
	}
	seen := make(map[string]struct{}, len(result.AnchorPhaseReceiptSHA256))
	for _, digest := range result.AnchorPhaseReceiptSHA256 {
		if !qualificationHexDigest(digest) {
			return errors.New("release packet phase receipt digest is malformed")
		}
		if _, duplicate := seen[digest]; duplicate {
			return errors.New("release packet repeats a phase receipt digest")
		}
		seen[digest] = struct{}{}
	}
	return nil
}

func runQualificationPacketVerify(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("qualification-packet-verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	packetPath := flags.String("packet", "", "signed Level 5 release packet")
	releasePublicKeyPath := flags.String("release-public-key", "", "independent Ed25519 release verification key")
	durabilityPath := flags.String("durability-receipt", "", "exact durability receipt bound by the packet")
	soakPath := flags.String("soak-receipt", "", "exact soak receipt bound by the packet")
	destructivePath := flags.String("destructive-record", "", "exact destructive manifest bound by the packet")
	environmentPath := flags.String("environment-record", "", "exact qualification environment evidence bound by the destructive manifest")
	artifactsRootPath := flags.String("artifacts-root", "", "exact secured artifact directory bound through the destructive manifest")
	artifactsIndexPath := flags.String("artifacts-index", "", "exact secured artifact index bound by the destructive manifest")
	anchorPublicKeyPath := flags.String("anchor-public-key", "", "rollback-anchor evidence verification key")
	anchorHistoryReceiptPath := flags.String("anchor-history-receipt", "", "exact history receipt bound by the packet")
	anchorHistoryPath := flags.String("anchor-history", "", "exact schema-3 history controller bound by the packet")
	sourceRevision := flags.String("source-revision", "", "qualified 40- or 64-hex release revision")
	var phasePaths anchorReceiptFlags
	flags.Var(&phasePaths, "anchor-phase-receipt", "ordered exact phase receipt; repeat five times")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *packetPath == "" || *releasePublicKeyPath == "" || flags.NArg() != 0 {
		return errors.New("qualification-packet-verify requires packet, release public key and complete original Level 5 evidence")
	}
	var packet qualificationReleasePacket
	packetRaw, err := readQualificationReceipt(*packetPath, &packet)
	if err != nil {
		return err
	}
	publicKey, err := loadAnchorQualificationPublicKey(*releasePublicKeyPath)
	if err != nil {
		return err
	}
	if err := verifyQualificationReleasePacket(packet, publicKey); err != nil {
		return err
	}
	buildRevision, buildModified := qualificationPacketBuildIdentity()
	if buildRevision != packet.Qualification.SourceRevision || buildModified {
		return errors.New("offline verification requires a clean verifier binary built from the qualified revision")
	}
	recomputed, err := buildQualificationCheckResult(qualificationCheckInputs{
		durabilityPath: *durabilityPath, soakPath: *soakPath, destructivePath: *destructivePath,
		environmentPath: *environmentPath, artifactsRootPath: *artifactsRootPath, artifactsIndexPath: *artifactsIndexPath,
		anchorPublicKeyPath: *anchorPublicKeyPath, anchorPhasePaths: phasePaths,
		anchorHistoryReceiptPath: *anchorHistoryReceiptPath, anchorHistoryPath: *anchorHistoryPath, sourceRevision: *sourceRevision,
	})
	if err != nil {
		return fmt.Errorf("recompute Level 5 evidence: %w", err)
	}
	if !reflect.DeepEqual(recomputed, packet.Qualification) {
		return errors.New("release packet qualification differs from recomputed original evidence")
	}
	result := struct {
		SchemaVersion    uint32 `json:"schemaVersion"`
		SourceRevision   string `json:"sourceRevision"`
		PacketSHA256     string `json:"packetSha256"`
		ReleaseKeySHA256 string `json:"releaseKeySha256"`
		Passed           bool   `json:"passed"`
	}{1, packet.Qualification.SourceRevision, qualificationSHA256(packetRaw), qualificationSHA256(publicKey), true}
	return json.NewEncoder(stdout).Encode(result)
}
