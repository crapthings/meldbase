package primarylease

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"github.com/crapthings/meldbase"
)

// ErrPrimaryRuntimeConfiguration reports settings that cannot safely connect a database, write guard, and renewer.
//
// In particular, callers must not inject an unrelated write fence alongside a
// Guard that a Renewer will update.
var ErrPrimaryRuntimeConfiguration = errors.New("meldbase primary lease: invalid primary runtime configuration")

// PrimaryOptions configures a primary database and its write lease renewal.
//
// The database uses the exact Guard instance its Renewer will update.
// OpenOptions remains available for storage settings,
// but PrimaryWriteFence and Follower are owned by this constructor and must be
// left unset/false.
type PrimaryOptions struct {
	OpenOptions     meldbase.OpenOptions
	PublicKey       ed25519.PublicKey
	GuardOptions    GuardOptions
	RenewalClient   RenewalClient
	RequestTimeout  time.Duration
	RetryInterval   time.Duration
	KeepLeaseOnStop bool
}

// PrimaryRuntime groups a primary database with its write guard and lease renewer.
//
// The database starts
// closed to writes because Guard starts without a certificate; call Renew once
// explicitly or Run under a long-lived supervisor before serving mutations.
type PrimaryRuntime struct {
	DB      *meldbase.DB
	Guard   *Guard
	Renewer *Renewer
}

// OpenPrimary opens a primary database with a shared write guard and lease renewer.
//
// The new Guard is the database's only PrimaryWriteFence. It does
// not contact the controller or start a goroutine; callers choose the startup
// ordering and context by calling Renew or Run.
func OpenPrimary(path string, options PrimaryOptions) (*PrimaryRuntime, error) {
	if options.RenewalClient == nil || options.OpenOptions.PrimaryWriteFence != nil || options.OpenOptions.Follower {
		return nil, ErrPrimaryRuntimeConfiguration
	}
	guard, err := NewGuard(options.PublicKey, options.GuardOptions)
	if err != nil {
		return nil, err
	}
	store := options.OpenOptions
	store.PrimaryWriteFence = guard
	db, err := meldbase.OpenWithOptions(path, store)
	if err != nil {
		return nil, err
	}
	renewer, err := NewRenewer(RenewerOptions{
		DB: db, Guard: guard, Client: options.RenewalClient,
		RequestTimeout: options.RequestTimeout, RetryInterval: options.RetryInterval,
		KeepLeaseOnStop: options.KeepLeaseOnStop, Clock: options.GuardOptions.Clock,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PrimaryRuntime{DB: db, Guard: guard, Renewer: renewer}, nil
}

// Renew acquires and installs a write lease through the primary's matching renewer.
func (runtime *PrimaryRuntime) Renew(ctx context.Context) error {
	if runtime == nil || runtime.Renewer == nil {
		return ErrPrimaryRuntimeConfiguration
	}
	return runtime.Renewer.Renew(ctx)
}

// Run renews the primary's write lease until the context ends.
//
// By default, returning revokes the local Guard.
func (runtime *PrimaryRuntime) Run(ctx context.Context) error {
	if runtime == nil || runtime.Renewer == nil {
		return ErrPrimaryRuntimeConfiguration
	}
	return runtime.Renewer.Run(ctx)
}

// Close immediately closes local primary admission before closing the database.
func (runtime *PrimaryRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	if runtime.Guard != nil {
		runtime.Guard.Revoke()
	}
	if runtime.DB == nil {
		return nil
	}
	return runtime.DB.Close()
}
