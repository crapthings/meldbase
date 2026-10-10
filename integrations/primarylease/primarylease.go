// Package primarylease supplies a locally verifiable, short-lived primary
// write lease for Meldbase  deployments.
//
// It intentionally does not elect a leader or persist distributed controller
// state. A separately operated controller must atomically fence an old owner
// before signing a lease for a new one. The guard keeps the database commit
// path free of network I/O: it verifies a controller-signed certificate when
// it is installed, then performs only bounded local checks for each write.
package primarylease

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/crapthings/meldbase"
)

const (
	// certificateVersion identifies the signed certificate payload layout.
	certificateVersion byte = 1
	// certificatePrefix identifies primary lease certificate payloads.
	certificatePrefix = "MLP"
	// maxOwnerBytes limits the byte length of a certificate owner identifier.
	maxOwnerBytes = 48
	// maxCertificateBytes bounds the binary payload and signature size.
	maxCertificateBytes = 4 + 16 + 1 + maxOwnerBytes + 8 + 8 + 8 + 8 + ed25519.SignatureSize
	// defaultMaxLeaseDuration sets the guard's default accepted lease lifetime.
	defaultMaxLeaseDuration = 30 * time.Second
	// hardMaxLeaseDuration bounds the configurable accepted lease lifetime.
	hardMaxLeaseDuration = 10 * time.Minute
	// maxCertificateText bounds the URL-safe base64 length of a signed certificate.
	maxCertificateText = 4 * ((maxCertificateBytes + 2) / 3)
)

var (
	// ErrCertificate reports a malformed, invalid or unsuitable lease
	// certificate.
	//
	// It intentionally gives no signature-verification detail.
	ErrCertificate = errors.New("meldbase primary lease: invalid certificate")
	// ErrLeaseExpired reports a certificate that is not currently valid.
	ErrLeaseExpired = errors.New("meldbase primary lease: lease is not currently valid")
	// ErrLeaseOwner reports a lease issued for a different configured owner.
	ErrLeaseOwner = errors.New("meldbase primary lease: certificate owner mismatch")
	// ErrLeaseEpoch reports an attempted local rollback or replay of a fenced
	// controller epoch.
	ErrLeaseEpoch = errors.New("meldbase primary lease: stale controller epoch")
)

// Certificate gives a database owner signed permission to accept writes for a limited time.
//
// CommitSequence is the source position at which the authority
// was issued; the first accepted write must advance beyond it. Epoch must grow
// monotonically in the controller, but the guard treats it as opaque ordering
// data because it cannot contact the controller in the write path.
type Certificate struct {
	DatabaseID     [16]byte
	Owner          string
	Epoch          uint64
	CommitSequence uint64
	NotBefore      time.Time
	NotAfter       time.Time
}

// Sign creates a signed, URL-safe certificate using the controller's private key.
//
// The result is suitable for FollowerPromotionFence.Epoch. privateKey belongs
// only to the controller; a
// database process needs the corresponding public key.
func Sign(certificate Certificate, privateKey ed25519.PrivateKey) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", ErrCertificate
	}
	payload, err := marshalCertificate(certificate)
	if err != nil {
		return "", err
	}
	signature := ed25519.Sign(privateKey, payload)
	return base64.RawURLEncoding.EncodeToString(append(payload, signature...)), nil
}

// Parse verifies and decodes an opaque signed certificate.
//
// The returned value
// has millisecond precision because that is the signed wire representation.
func Parse(encoded string, publicKey ed25519.PublicKey) (Certificate, error) {
	if len(publicKey) != ed25519.PublicKeySize || len(encoded) == 0 || len(encoded) > maxCertificateText {
		return Certificate{}, ErrCertificate
	}
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(wire) < 4+16+1+8+8+8+8+ed25519.SignatureSize || len(wire) > maxCertificateBytes {
		return Certificate{}, ErrCertificate
	}
	// Reject alternate base64 spellings that decode to the same bytes. Without
	// this canonicality check, unused bits in the final unpadded character can
	// make a visibly modified promotion fence retain a valid signature.
	if base64.RawURLEncoding.EncodeToString(wire) != encoded {
		return Certificate{}, ErrCertificate
	}
	payload, signature := wire[:len(wire)-ed25519.SignatureSize], wire[len(wire)-ed25519.SignatureSize:]
	if !ed25519.Verify(publicKey, payload, signature) {
		return Certificate{}, ErrCertificate
	}
	return unmarshalCertificate(payload)
}

// GuardOptions sets the local write owner, maximum lease duration, and clock.
//
// Owner is a stable,
// controller-defined process identity; it is not supplied by an untrusted
// client. Clock is intended for deterministic tests; production callers leave
// it nil.
type GuardOptions struct {
	Owner            string
	MaxLeaseDuration time.Duration
	Clock            func() time.Time
}

// Guard permits database writes only while a valid certificate authorizes its configured owner.
//
// It implements meldbase.PrimaryWriteFence and meldbase.FollowerPromotionFenceBinder.
// Install is used for an already
// elected primary; follower promotion binds the certificate returned by the
// external promotion authority automatically.
type Guard struct {
	mu           sync.RWMutex
	publicKey    ed25519.PublicKey
	owner        string
	now          func() time.Time
	maxLease     time.Duration
	lease        Certificate
	minimumEpoch uint64
	installed    bool
}

// GuardLeaseStatus reports the installed lease position and time window for renewal scheduling.
//
// It contains no certificate text or owner identity. Installed does not imply the
// lease is still time-valid; callers evaluate NotAfter against their own
// trusted local clock.
type GuardLeaseStatus struct {
	Installed      bool
	Epoch          uint64
	CommitSequence uint64
	NotBefore      time.Time
	NotAfter       time.Time
}

// NewGuard creates a write guard that rejects writes until a valid certificate is installed.
//
// Install or follower promotion binds the certificate.
func NewGuard(publicKey ed25519.PublicKey, options GuardOptions) (*Guard, error) {
	if len(publicKey) != ed25519.PublicKeySize || !validOwner(options.Owner) {
		return nil, ErrCertificate
	}
	maxLease := options.MaxLeaseDuration
	if maxLease == 0 {
		maxLease = defaultMaxLeaseDuration
	}
	if maxLease < time.Second || maxLease > hardMaxLeaseDuration {
		return nil, ErrCertificate
	}
	now := options.Clock
	if now == nil {
		now = time.Now
	}
	return &Guard{publicKey: append(ed25519.PublicKey(nil), publicKey...), owner: options.Owner, now: now, maxLease: maxLease}, nil
}

// Install verifies and atomically replaces the locally accepted lease.
//
// An
// external controller renews authority by signing a new certificate and
// calling Install before the old one expires. Revocation clears it locally.
func (guard *Guard) Install(encoded string) error {
	if guard == nil {
		return ErrCertificate
	}
	certificate, err := Parse(encoded, guard.publicKey)
	if err != nil {
		return err
	}
	if err := guard.acceptable(certificate, guard.now()); err != nil {
		return err
	}
	guard.mu.Lock()
	if err := guard.acceptEpochLocked(certificate); err != nil {
		guard.mu.Unlock()
		return err
	}
	guard.lease, guard.installed = certificate, true
	guard.mu.Unlock()
	return nil
}

// Revoke immediately blocks local database writes.
//
// It is safe to call more
// than once. A partitioned old primary remains bounded by NotAfter even if it
// never receives this local revocation.
func (guard *Guard) Revoke() {
	if guard == nil {
		return
	}
	guard.mu.Lock()
	if guard.installed && guard.lease.Epoch < ^uint64(0) {
		guard.minimumEpoch = guard.lease.Epoch + 1
	}
	guard.lease, guard.installed = Certificate{}, false
	guard.mu.Unlock()
}

// LeaseStatus returns the installed lease position and time window without allocating.
//
// It runs in O(1) and is for
// a renewal supervisor's scheduling and must never replace the hot-path write
// fence, which validates the certificate again for every commit.
func (guard *Guard) LeaseStatus() GuardLeaseStatus {
	if guard == nil {
		return GuardLeaseStatus{}
	}
	guard.mu.RLock()
	certificate, installed := guard.lease, guard.installed
	guard.mu.RUnlock()
	if !installed {
		return GuardLeaseStatus{}
	}
	return GuardLeaseStatus{Installed: true, Epoch: certificate.Epoch, CommitSequence: certificate.CommitSequence, NotBefore: certificate.NotBefore, NotAfter: certificate.NotAfter}
}

// ValidatePrimaryWrite checks that the installed lease permits the next database commit.
//
// It performs no I/O,
// allocation or controller call; its only synchronization is an RLock around
// the installed immutable certificate.
func (guard *Guard) ValidatePrimaryWrite(request meldbase.PrimaryWriteFenceRequest) error {
	if guard == nil {
		return ErrCertificate
	}
	guard.mu.RLock()
	certificate, installed := guard.lease, guard.installed
	guard.mu.RUnlock()
	if !installed || guard.acceptable(certificate, guard.now()) != nil || certificate.DatabaseID != request.DatabaseID || request.NextCommitSequence <= certificate.CommitSequence {
		return ErrCertificate
	}
	return nil
}

// BindFollowerPromotion verifies that the authority's opaque epoch is a
// certificate for this exact promotion point before enabling local writes.
func (guard *Guard) BindFollowerPromotion(ctx context.Context, fence meldbase.FollowerPromotionFence) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	certificate, err := Parse(fence.Epoch, guard.publicKey)
	if err != nil {
		return err
	}
	if certificate.DatabaseID != fence.DatabaseID || certificate.CommitSequence != fence.CommitSequence {
		return ErrCertificate
	}
	if err := guard.acceptable(certificate, guard.now()); err != nil {
		return err
	}
	guard.mu.Lock()
	if err := guard.acceptEpochLocked(certificate); err != nil {
		guard.mu.Unlock()
		return err
	}
	guard.lease, guard.installed = certificate, true
	guard.mu.Unlock()
	return nil
}

// acceptable checks a certificate's owner, identity, epoch, duration, and current validity.
func (guard *Guard) acceptable(certificate Certificate, now time.Time) error {
	if !validOwner(certificate.Owner) || subtle.ConstantTimeCompare([]byte(certificate.Owner), []byte(guard.owner)) != 1 {
		return ErrLeaseOwner
	}
	if certificate.DatabaseID == [16]byte{} || certificate.Epoch == 0 || certificate.Epoch == ^uint64(0) || certificate.NotBefore.IsZero() || certificate.NotAfter.IsZero() || !certificate.NotAfter.After(certificate.NotBefore) {
		return ErrCertificate
	}
	if certificate.NotAfter.Sub(certificate.NotBefore) > guard.maxLease {
		return ErrCertificate
	}
	now = now.UTC().Truncate(time.Millisecond)
	if now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) {
		return ErrLeaseExpired
	}
	return nil
}

// acceptEpochLocked rejects revoked or older epochs and inconsistent renewals while the caller holds the guard lock.
func (guard *Guard) acceptEpochLocked(certificate Certificate) error {
	if certificate.Epoch < guard.minimumEpoch {
		return ErrLeaseEpoch
	}
	if !guard.installed {
		return nil
	}
	if certificate.Epoch < guard.lease.Epoch {
		return ErrLeaseEpoch
	}
	if certificate.Epoch == guard.lease.Epoch && (certificate.DatabaseID != guard.lease.DatabaseID || certificate.Owner != guard.lease.Owner || certificate.CommitSequence != guard.lease.CommitSequence) {
		return ErrLeaseEpoch
	}
	return nil
}

// marshalCertificate validates and encodes the certificate fields covered by its signature.
func marshalCertificate(certificate Certificate) ([]byte, error) {
	if certificate.DatabaseID == [16]byte{} || !validOwner(certificate.Owner) || certificate.Epoch == 0 || certificate.Epoch == ^uint64(0) || certificate.NotBefore.IsZero() || certificate.NotAfter.IsZero() {
		return nil, ErrCertificate
	}
	notBefore, notAfter := certificate.NotBefore.UTC().UnixMilli(), certificate.NotAfter.UTC().UnixMilli()
	if notAfter <= notBefore {
		return nil, ErrCertificate
	}
	payload := make([]byte, 4+16+1+len(certificate.Owner)+8+8+8+8)
	copy(payload, certificatePrefix)
	payload[3] = certificateVersion
	copy(payload[4:20], certificate.DatabaseID[:])
	payload[20] = byte(len(certificate.Owner))
	offset := 21
	offset += copy(payload[offset:], certificate.Owner)
	binary.BigEndian.PutUint64(payload[offset:offset+8], certificate.Epoch)
	offset += 8
	binary.BigEndian.PutUint64(payload[offset:offset+8], certificate.CommitSequence)
	offset += 8
	binary.BigEndian.PutUint64(payload[offset:offset+8], uint64(notBefore))
	offset += 8
	binary.BigEndian.PutUint64(payload[offset:offset+8], uint64(notAfter))
	return payload, nil
}

// unmarshalCertificate validates and decodes a certificate payload without checking its signature.
func unmarshalCertificate(payload []byte) (Certificate, error) {
	if len(payload) < 4+16+1+8+8+8+8 || string(payload[:3]) != certificatePrefix || payload[3] != certificateVersion {
		return Certificate{}, ErrCertificate
	}
	ownerLength := int(payload[20])
	want := 4 + 16 + 1 + ownerLength + 8 + 8 + 8 + 8
	if ownerLength == 0 || ownerLength > maxOwnerBytes || len(payload) != want {
		return Certificate{}, ErrCertificate
	}
	var certificate Certificate
	copy(certificate.DatabaseID[:], payload[4:20])
	certificate.Owner = string(payload[21 : 21+ownerLength])
	offset := 21 + ownerLength
	certificate.Epoch = binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8
	certificate.CommitSequence = binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8
	certificate.NotBefore = time.UnixMilli(int64(binary.BigEndian.Uint64(payload[offset : offset+8]))).UTC()
	offset += 8
	certificate.NotAfter = time.UnixMilli(int64(binary.BigEndian.Uint64(payload[offset : offset+8]))).UTC()
	if !validOwner(certificate.Owner) || certificate.DatabaseID == [16]byte{} || certificate.Epoch == 0 || certificate.Epoch == ^uint64(0) || !certificate.NotAfter.After(certificate.NotBefore) {
		return Certificate{}, ErrCertificate
	}
	return certificate, nil
}

// validOwner checks the length and allowed ASCII characters of a lease owner identifier.
func validOwner(value string) bool {
	if len(value) == 0 || len(value) > maxOwnerBytes {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.') {
			return false
		}
	}
	return true
}

// contextError rejects a missing context or returns its cancellation error.
func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("meldbase primary lease: context is required")
	}
	return ctx.Err()
}

// CertificateTextLimit is the maximum encoded certificate length.
//
// It remains
// below Meldbase's promotion-fence epoch limit.
func CertificateTextLimit() int { return maxCertificateText }

// Guard must satisfy the database write-fence contract.
var _ meldbase.PrimaryWriteFence = (*Guard)(nil)

// Guard must satisfy the follower promotion binding contract.
var _ meldbase.FollowerPromotionFenceBinder = (*Guard)(nil)

// String returns the certificate's epoch and commit position without exposing its owner or signed text.
func (certificate Certificate) String() string {
	return fmt.Sprintf("primary-lease(epoch=%d, sequence=%d)", certificate.Epoch, certificate.CommitSequence)
}
