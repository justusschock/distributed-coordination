package coordination

import (
	"context"
	"time"
)

type Member struct {
	Group    string
	ID       string
	Metadata map[string]string
}

type MemberInfo struct {
	Member    Member
	JoinedAt  time.Time
	ExpiresAt time.Time
}

type Membership interface {
	Info() MemberInfo
	Done() <-chan struct{}
	Renew(context.Context, time.Duration) (MemberInfo, error)
	Leave(context.Context) error
}

type Registry interface {
	Join(context.Context, Member, time.Duration) (Membership, error)
	List(context.Context, string) ([]MemberInfo, error)
}
