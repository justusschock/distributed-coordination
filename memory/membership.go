package memory

import (
	"context"
	"sort"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
)

type memberRecord struct {
	info coordination.MemberInfo
	done chan struct{}
	lost bool
}

type membership struct {
	backend *Backend
	record  *memberRecord
}

// Join implements [coordination.Registry]. Member metadata is copied on the
// way in and on every read.
func (b *Backend) Join(ctx context.Context, member coordination.Member, ttl time.Duration) (coordination.Membership, error) {
	if member.Group == "" || member.ID == "" || ttl <= 0 {
		return nil, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	now := b.clock.Now()
	group := b.members[member.Group]
	if group == nil {
		group = make(map[string]*memberRecord)
		b.members[member.Group] = group
	}
	if current := group[member.ID]; current != nil {
		if current.info.ExpiresAt.After(now) {
			b.mu.Unlock()
			return nil, coordination.ErrNotAcquired
		}
		b.loseMemberLocked(current)
	}

	current := &memberRecord{
		info: coordination.MemberInfo{
			Member: coordination.Member{
				Group:    member.Group,
				ID:       member.ID,
				Metadata: copyMetadata(member.Metadata),
			},
			JoinedAt:  now,
			ExpiresAt: now.Add(ttl),
		},
		done: make(chan struct{}),
	}
	group[member.ID] = current
	b.members[member.Group] = group
	b.mu.Unlock()

	b.scheduleMemberExpiry(current, ttl)
	return &membership{backend: b, record: current}, nil
}

// List implements [coordination.Registry]. It returns copies; mutating them
// does not affect the Backend.
func (b *Backend) List(ctx context.Context, group string) ([]coordination.MemberInfo, error) {
	if group == "" {
		return nil, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	records := b.members[group]
	result := make([]coordination.MemberInfo, 0, len(records))
	for _, current := range records {
		if !current.info.ExpiresAt.After(now) {
			b.loseMemberLocked(current)
			continue
		}
		result = append(result, copyMemberInfo(current.info))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Member.ID < result[j].Member.ID
	})
	return result, nil
}

func (b *Backend) scheduleMemberExpiry(current *memberRecord, delay time.Duration) {
	expiry := b.clock.After(delay)
	go func() {
		select {
		case <-current.done:
		case <-expiry:
			b.expireMember(current)
		}
	}()
}

func (b *Backend) expireMember(current *memberRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.memberRecordLocked(current.info.Member.Group, current.info.Member.ID) != current || current.lost {
		return
	}
	if current.info.ExpiresAt.After(b.clock.Now()) {
		return
	}
	b.loseMemberLocked(current)
}

func (b *Backend) loseMemberLocked(current *memberRecord) {
	groupName := current.info.Member.Group
	group := b.members[groupName]
	if group == nil || group[current.info.Member.ID] != current || current.lost {
		return
	}
	current.lost = true
	delete(group, current.info.Member.ID)
	if len(group) == 0 {
		delete(b.members, groupName)
	}
	close(current.done)
}

func (b *Backend) memberRecordLocked(group, id string) *memberRecord {
	if b.members[group] == nil {
		return nil
	}
	return b.members[group][id]
}

func (m *membership) Info() coordination.MemberInfo {
	m.backend.mu.Lock()
	defer m.backend.mu.Unlock()
	return copyMemberInfo(m.record.info)
}

func (m *membership) Done() <-chan struct{} {
	return m.record.done
}

func (m *membership) Renew(ctx context.Context, ttl time.Duration) (coordination.MemberInfo, error) {
	if ttl <= 0 {
		return coordination.MemberInfo{}, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return coordination.MemberInfo{}, err
	}

	b := m.backend
	b.mu.Lock()
	current := b.memberRecordLocked(m.record.info.Member.Group, m.record.info.Member.ID)
	if current != m.record || current.lost {
		b.mu.Unlock()
		return coordination.MemberInfo{}, coordination.ErrLeaseLost
	}
	now := b.clock.Now()
	if !current.info.ExpiresAt.After(now) {
		b.loseMemberLocked(current)
		b.mu.Unlock()
		return coordination.MemberInfo{}, coordination.ErrLeaseLost
	}
	current.info.ExpiresAt = now.Add(ttl)
	info := copyMemberInfo(current.info)
	b.mu.Unlock()

	b.scheduleMemberExpiry(current, ttl)
	return info, nil
}

func (m *membership) Leave(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b := m.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.memberRecordLocked(m.record.info.Member.Group, m.record.info.Member.ID)
	if current != m.record || current.lost {
		return coordination.ErrLeaseLost
	}
	if !current.info.ExpiresAt.After(b.clock.Now()) {
		b.loseMemberLocked(current)
		return coordination.ErrLeaseLost
	}
	b.loseMemberLocked(current)
	return nil
}

func copyMemberInfo(info coordination.MemberInfo) coordination.MemberInfo {
	info.Member.Metadata = copyMetadata(info.Member.Metadata)
	return info
}

func copyMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		result[key] = value
	}
	return result
}
