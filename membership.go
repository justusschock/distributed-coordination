package coordination

import (
	"context"
	"time"
)

// Member identifies a participant in a group.
type Member struct {
	// Group names the group. It must not be empty.
	Group string
	// ID identifies the member within the group. It must not be empty.
	ID string
	// Metadata is optional, opaque data published with the member.
	// Registries copy it defensively.
	Metadata map[string]string
}

// MemberInfo is a snapshot of an active membership.
type MemberInfo struct {
	Member    Member
	JoinedAt  time.Time
	ExpiresAt time.Time
}

// Membership is an expiring registration of one member. Implementations must
// be safe for concurrent use.
type Membership interface {
	// Info returns the latest snapshot of the membership.
	Info() MemberInfo
	// Done is closed exactly once when the membership is left or expires.
	Done() <-chan struct{}
	// Renew extends the deadline to the registry clock's current time plus
	// ttl. It returns [ErrInvalidRequest] for a non-positive ttl and
	// [ErrLeaseLost] after leave, expiry or replacement.
	Renew(ctx context.Context, ttl time.Duration) (MemberInfo, error)
	// Leave ends the membership and closes Done. It returns [ErrLeaseLost]
	// if the membership already ended.
	Leave(ctx context.Context) error
}

// Registry tracks expiring group membership. Implementations must be safe for
// concurrent use.
type Registry interface {
	// Join registers member for ttl. It returns [ErrInvalidRequest] for an
	// empty group or ID or a non-positive ttl and [ErrNotAcquired] while the
	// same group and ID are already active. After leave or expiry the same
	// ID may join again as a fresh membership.
	Join(ctx context.Context, member Member, ttl time.Duration) (Membership, error)
	// List returns the active members of group sorted by ID. It returns
	// [ErrInvalidRequest] for an empty group.
	List(ctx context.Context, group string) ([]MemberInfo, error)
}
