package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	r := New()

	n := 0
	err := r(context.Background(), func(ctx context.Context) error {
		n++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	n = 0
	err = r(context.Background(), func(ctx context.Context) error {
		n++
		if n < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n = 0
	err = r(context.Background(), func(ctx context.Context) error {
		n++
		return fmt.Errorf("wrapped: %w", ErrStop)
	})
	assert.ErrorIs(t, err, ErrStop)
	assert.Equal(t, 1, n)
}

func TestExplicit(t *testing.T) {
	r := NewExplicit(true)

	n := 0
	err := r(context.Background(), func(ctx context.Context) error {
		n++
		return errors.New("boom")
	})
	require.Error(t, err)
	assert.Equal(t, 1, n, "should stop on first non-retry error")

	n = 0
	err = r(context.Background(), func(ctx context.Context) error {
		n++
		if n < 4 {
			return fmt.Errorf("retry pls: %w", ErrRetry)
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 4, n)
}

func TestMaxAttempts(t *testing.T) {
	n := 0
	r := New(MaxAttempts(3))
	err := r(context.Background(), func(ctx context.Context) error {
		n++
		return errors.New("nope")
	})
	require.ErrorIs(t, err, ErrStop)
	assert.Equal(t, 3, n)

	// succeeds on 2nd try
	n = 0
	r = New(MaxAttempts(10))
	err = r(context.Background(), func(ctx context.Context) error {
		n++
		if n == 2 {
			return nil
		}
		return errors.New("retry")
	})
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestDelay(t *testing.T) {
	r := New(MaxAttempts(3), Delay(DelayOptions{Delay: 10 * time.Millisecond}))
	start := time.Now()
	_ = r(context.Background(), func(ctx context.Context) error {
		return errors.New("fail")
	})
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
}

func TestDelayExponential(t *testing.T) {
	r := New(MaxAttempts(3), Delay(DelayOptions{
		Delay: 5 * time.Millisecond,
		Func:  DoubleDelay,
	}))
	start := time.Now()
	_ = r(context.Background(), func(ctx context.Context) error {
		return errors.New("fail")
	})
	assert.GreaterOrEqual(t, time.Since(start), 15*time.Millisecond)
}

func TestDelayMax(t *testing.T) {
	r := New(MaxAttempts(4), Delay(DelayOptions{
		Delay: 5 * time.Millisecond,
		Func:  DoubleDelay,
		Max:   7 * time.Millisecond,
	}))
	start := time.Now()
	_ = r(context.Background(), func(ctx context.Context) error {
		return errors.New("fail")
	})
	assert.GreaterOrEqual(t, time.Since(start), 19*time.Millisecond)
}

func TestTimeout(t *testing.T) {
	r := New(Timeout(30 * time.Millisecond))
	err := r(context.Background(), func(ctx context.Context) error {
		time.Sleep(15 * time.Millisecond)
		return errors.New("slow")
	})
	require.ErrorIs(t, err, ErrStop)

	n := 0
	r = New(Timeout(100 * time.Millisecond))
	err = r(context.Background(), func(ctx context.Context) error {
		n++
		if n == 2 {
			return nil
		}
		return errors.New("once more")
	})
	require.NoError(t, err)
}

func TestExponential(t *testing.T) {
	assert.Equal(t, 20*time.Millisecond, DoubleDelay(10*time.Millisecond))
	assert.Equal(t, 30*time.Millisecond, Exponential(3)(10*time.Millisecond))
}
