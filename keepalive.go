package coordination

import (
	"context"
	"time"

	"github.com/justusschock/go-concurrency"
)

func KeepAlive(ctx context.Context, lease Lease, ttl time.Duration) (*concurrency.Future[LeaseInfo], error) {
	if lease == nil || ttl <= 0 {
		return nil, ErrInvalidRequest
	}

	future := concurrency.Go(ctx, func(ctx context.Context) (LeaseInfo, error) {
		latest := lease.Info()
		for {
			select {
			case <-ctx.Done():
				return latest, context.Cause(ctx)
			case <-lease.Done():
				return latest, ErrLeaseLost
			default:
			}

			info, err := lease.Renew(ctx, ttl)
			if err != nil {
				return latest, err
			}
			latest = info

			timer := time.NewTimer(ttl / 3)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return latest, context.Cause(ctx)
			case <-lease.Done():
				stopTimer(timer)
				return latest, ErrLeaseLost
			case <-timer.C:
			}
		}
	})
	return future, nil
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
