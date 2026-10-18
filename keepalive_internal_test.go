package coordination

import (
	"testing"
	"time"
)

// FR-COORD-06
func TestKeepAliveIntervalIsAlwaysPositive(t *testing.T) {
	tests := []struct {
		ttl  time.Duration
		want time.Duration
	}{
		{ttl: time.Nanosecond, want: time.Nanosecond},
		{ttl: 2 * time.Nanosecond, want: time.Nanosecond},
		{ttl: 3 * time.Nanosecond, want: time.Nanosecond},
		{ttl: 6 * time.Nanosecond, want: 2 * time.Nanosecond},
	}
	for _, test := range tests {
		if got := keepAliveInterval(test.ttl); got != test.want {
			t.Errorf("keepAliveInterval(%v) = %v, want %v", test.ttl, got, test.want)
		}
	}
}
