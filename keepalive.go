package coordination

import (
	"context"
	"time"

	"github.com/justusschock/go-concurrency"
)

// KeepAlive renews lease with ttl immediately and then every ttl/3 (at least
// every nanosecond) on its own goroutine. The returned future resolves with
// the latest [LeaseInfo] and
//
//   - the cancellation cause of ctx when ctx is done,
//   - [ErrLeaseLost] when lease.Done closes, or
//   - the first error returned by Renew.
//
// Cancel ctx to stop renewing; the lease itself is not released. KeepAlive
// returns [ErrInvalidRequest] for a nil lease or a non-positive ttl.
func KeepAlive(ctx context.Context, lease Lease, ttl time.Duration) (*concurrency.Future[LeaseInfo], error) {
	if lease == nil || ttl <= 0 {
		return nil, ErrInvalidRequest
	}
	interval := keepAliveInterval(ttl)

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

			timer := time.NewTimer(interval)
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

func keepAliveInterval(ttl time.Duration) time.Duration {
	interval := ttl / 3
	if interval == 0 {
		return time.Nanosecond
	}
	return interval
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
