package sink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/andriibeee/iotdemo/internal/entity"
	apperr "github.com/andriibeee/iotdemo/internal/errors"
)

func TestDeduplicator(t *testing.T) {
	t.Run("passes unique events", func(t *testing.T) {
		var received []entity.Event
		handler := func(ev entity.Event) error {
			received = append(received, ev)
			return nil
		}

		d := NewDeduplicator(time.Hour)
		d.Start()
		mw := d.Middleware()(handler)

		mw(entity.Event{IdempotencyID: "a", Sensor: "temp", Value: 1})
		mw(entity.Event{IdempotencyID: "b", Sensor: "temp", Value: 2})
		mw(entity.Event{IdempotencyID: "c", Sensor: "temp", Value: 3})

		assert.Len(t, received, 3)
	})

	t.Run("returns error for duplicates", func(t *testing.T) {
		var received []entity.Event
		handler := func(ev entity.Event) error {
			received = append(received, ev)
			return nil
		}

		d := NewDeduplicator(time.Hour)
		d.Start()
		mw := d.Middleware()(handler)

		err1 := mw(entity.Event{IdempotencyID: "same", Sensor: "temp", Value: 1})
		err2 := mw(entity.Event{IdempotencyID: "same", Sensor: "temp", Value: 2})
		err3 := mw(entity.Event{IdempotencyID: "same", Sensor: "temp", Value: 3})

		assert.NoError(t, err1)
		assert.ErrorIs(t, err2, apperr.ErrDuplicate)
		assert.ErrorIs(t, err3, apperr.ErrDuplicate)
		assert.Len(t, received, 1)
	})

}

func TestDeduplicatorWithSink(t *testing.T) {
	ctrl := gomock.NewController(t)
	j := NewMockJournal(ctrl)
	j.EXPECT().WriteBatch(gomock.Any()).Return(nil, nil).AnyTimes()

	d := NewDeduplicator(time.Hour)
	d.Start()
	s := New(j, WithBufSize(10), WithMiddleware(d.Middleware()))

	require.NoError(t, s.Append(entity.Event{IdempotencyID: "x", Sensor: "temp", Value: 1}))
	assert.ErrorIs(t, s.Append(entity.Event{IdempotencyID: "x", Sensor: "temp", Value: 2}), apperr.ErrDuplicate)
	require.NoError(t, s.Append(entity.Event{IdempotencyID: "y", Sensor: "temp", Value: 3}))

	s.flush()

	assert.Equal(t, uint(2), d.Count())
}

func TestDeduplicatorCleaning(t *testing.T) {
	d := NewDeduplicator(10 * time.Millisecond)
	d.Start()
	mw := d.Middleware()(func(ev entity.Event) error { return nil })

	err1 := mw(entity.Event{IdempotencyID: "a"})
	assert.NoError(t, err1)
	assert.Equal(t, uint(1), d.Count())

	err2 := mw(entity.Event{IdempotencyID: "a"})
	assert.ErrorIs(t, err2, apperr.ErrDuplicate)

	// Wait for cleaning
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, uint(0), d.Count(), "counter should be reset")

	err3 := mw(entity.Event{IdempotencyID: "a"})
	assert.NoError(t, err3, "should be able to insert again after cleaning")
	assert.Equal(t, uint(1), d.Count())
}
