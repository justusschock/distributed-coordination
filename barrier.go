package coordination

import (
	"context"
	"time"
)

type BarrierRequest struct {
	Name        string
	Participant string
	Parties     int
	TTL         time.Duration
}

type BarrierResult struct {
	Name         string
	Generation   uint64
	Participants []string
}

type Barrier interface {
	Wait(context.Context, BarrierRequest) (BarrierResult, error)
}
