package primarylease

import (
	"context"
	"errors"

	"github.com/crapthings/meldbase"
)

// DurableConsumerPromotionReadiness permits promotion only when source and acknowledged follower positions match.
//
// It supports a single-writer follower. The source's authenticated named
// durable database consumer must have durably acknowledged exactly the
// candidate follower's local token, and the source must not have advanced beyond it.
//
// Authority runs readiness only after the old lease's skew-safe handoff point.
// The deployment must still ensure that the source used that lease guard and
// that ConsumerName is bound by its replication transport to this follower's
// trusted identity. It deliberately stays closed if the source is unavailable,
// history state is missing or either position differs.
type DurableConsumerPromotionReadiness struct {
	Source       *meldbase.DB
	ConsumerName string
	Buffer       int
}

// VerifyFollowerPromotion checks that the source and its durable consumer match the follower token.
func (readiness DurableConsumerPromotionReadiness) VerifyFollowerPromotion(ctx context.Context, request meldbase.FollowerPromotionRequest, record LeaseRecord, exists bool) error {
	if readiness.Source == nil || readiness.ConsumerName == "" || !exists || record.DatabaseID != request.DatabaseID || record.CommitSequence != request.CommitSequence {
		return ErrLeasePromotionReadiness
	}
	if readiness.Source.DatabaseID() != request.DatabaseID {
		return ErrLeasePromotionReadiness
	}
	buffer := readiness.Buffer
	if buffer == 0 {
		buffer = 1
	}
	subscription, err := readiness.Source.OpenDurableDatabaseChanges(ctx, readiness.ConsumerName, buffer)
	if err != nil {
		return errors.Join(ErrLeasePromotionReadiness, err)
	}
	defer subscription.Close()
	checkpoint, err := subscription.Checkpoint()
	if err != nil || checkpoint != request.CommitSequence {
		return errors.Join(ErrLeasePromotionReadiness, err)
	}
	if sourceToken := readiness.Source.Stats().CommitSequence; sourceToken != request.CommitSequence {
		return ErrLeasePromotionReadiness
	}
	return nil
}

// Verify at compile time that this check implements PromotionReadiness.
var _ PromotionReadiness = DurableConsumerPromotionReadiness{}
