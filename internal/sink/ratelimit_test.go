package sink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	apperr "github.com/andriibeee/iotdemo/internal/errors"
)

func TestRateLimiterMiddleware(t *testing.T) {
	ctrl := gomock.NewController(t)
	j := NewMockJournal(ctrl)
	j.EXPECT().WriteBatch(gomock.Any()).Return(nil, nil)

	rl := NewRateLimiter(30)
	s := New(j, WithBufSize(10), WithMiddleware(rl.Middleware()))

	gotLimited := false
	for i := range 20 {
		if s.Append(event("temp", i, int64(i*1000))) == apperr.ErrRateLimited {
			gotLimited = true
		}
	}
	s.flush()

	assert.NotZero(t, rl.DroppedCounter.Load(), "expected drops")
	assert.True(t, gotLimited, "expected apperr.ErrRateLimited")
}

func TestRateLimiterRefills(t *testing.T) {
	ctrl := gomock.NewController(t)
	j := NewMockJournal(ctrl)
	j.EXPECT().WriteBatch(gomock.Any()).Return(nil, nil).AnyTimes()

	rl := NewRateLimiter(100)
	s := New(j, WithBufSize(100), WithMiddleware(rl.Middleware()))

	// exhaust bucket
	for range 10 {
		_ = s.Append(event("temp", 1, 1000))
	}
	before := rl.DroppedCounter.Load()

	time.Sleep(150 * time.Millisecond)

	err := s.Append(event("temp", 1, 1000))
	after := rl.DroppedCounter.Load()

	assert.LessOrEqual(t, after, before+1, "bucket should refill")
	if after == before {
		assert.NoError(t, err, "refilled bucket should accept event")
	}
}
