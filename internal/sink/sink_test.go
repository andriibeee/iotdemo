package sink

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/andriibeee/iotdemo/internal/entity"
	"github.com/andriibeee/iotdemo/pkg/journal"
)

func newSink(t *testing.T, bufSize int, mw ...Middleware) (*Sink, *MockJournal) {
	ctrl := gomock.NewController(t)
	j := NewMockJournal(ctrl)
	opts := []Option{WithBufSize(bufSize)}
	if len(mw) > 0 {
		opts = append(opts, WithMiddleware(mw...))
	}
	return New(j, opts...), j
}

func event(sensor string, val int, ts int64) entity.Event {
	return entity.Event{Sensor: sensor, Value: val, UnixTimestamp: ts}
}

func TestFmtKey(t *testing.T) {
	s, _ := newSink(t, 1)

	f := func(sensor string, ts int64, want string) {
		t.Helper()
		got := string(s.fmtKey(sensor, ts))
		assert.Equal(t, want, got)
	}

	f("temp", 1234567890, "sensor_temp{ts=1234567890}")
	f("humidity", 0, "sensor_humidity{ts=0}")
}

func TestAppend(t *testing.T) {
	t.Run("writes dropped event from in memory buffer on overflow", func(t *testing.T) {
		s, j := newSink(t, 2)

		j.EXPECT().
			Write([]byte("sensor_temp{ts=1000}"), gomock.Any()).
			Return(uint64(1), nil)

		s.Append(event("temp", 1, 1000))
		s.Append(event("temp", 2, 2000))
		s.Append(event("temp", 3, 3000))
	})
}

func TestFlush(t *testing.T) {
	t.Run("writes buffered", func(t *testing.T) {
		s, j := newSink(t, 5)

		s.Append(event("temp", 20, 1000))
		s.Append(event("humidity", 65, 2000))

		j.EXPECT().WriteBatch(gomock.Len(2)).Return([]uint64{1, 2}, nil)
		require.NoError(t, s.flush())
	})
}

func TestRun(t *testing.T) {
	t.Run("stops on cancel", func(t *testing.T) {
		s, j := newSink(t, 5)
		j.EXPECT().WriteBatch(gomock.Any()).Return(nil, nil).AnyTimes()

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()

		time.Sleep(50 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Run didn't stop")
		}
	})

	t.Run("flushes on exit", func(t *testing.T) {
		s, j := newSink(t, 10)
		s.Append(event("temp", 42, 1000))

		j.EXPECT().WriteBatch(gomock.Any()).Return([]uint64{1}, nil).MinTimes(1)

		ctx, cancel := context.WithCancel(context.Background())
		go func() { s.Run(ctx) }()

		time.Sleep(50 * time.Millisecond)
		cancel()
		time.Sleep(50 * time.Millisecond)
	})
}

func TestClose(t *testing.T) {
	s, j := newSink(t, 5)
	s.Append(event("temp", 42, 1000))

	j.EXPECT().WriteBatch(gomock.Len(1)).Return([]uint64{1}, nil)
	require.NoError(t, s.Close())
}

func TestBufferOverflow(t *testing.T) {
	s, j := newSink(t, 2)

	j.EXPECT().Write(gomock.Any(), gomock.Any()).Return(uint64(1), nil).Times(3)

	for i := range 5 {
		require.NoError(t, s.Append(event("temp", i, int64(i*1000))))
	}
}

func TestFlushData(t *testing.T) {
	s, j := newSink(t, 5)

	for i := range 3 {
		s.Append(event("temp", i, int64(i*1000)))
	}

	j.EXPECT().
		WriteBatch(gomock.Any()).
		DoAndReturn(func(entries []journal.Entry) ([]uint64, error) {
			assert.Len(t, entries, 3)
			for i, e := range entries {
				assert.NotEmpty(t, e.Key, "entry %d key", i)
				assert.NotEmpty(t, e.Value, "entry %d value", i)
			}
			return []uint64{1, 2, 3}, nil
		})

	s.flush()
}

func TestMiddleware(t *testing.T) {
	t.Run("filter drops", func(t *testing.T) {
		dropNegative := func(next Handler) Handler {
			return func(ev entity.Event) error {
				if ev.Value < 0 {
					return nil
				}
				return next(ev)
			}
		}

		s, j := newSink(t, 5, dropNegative)
		j.EXPECT().WriteBatch(gomock.Len(2)).Return(nil, nil)

		s.Append(event("temp", 10, 1000))
		s.Append(event("temp", -5, 2000))
		s.Append(event("temp", 20, 3000))
		s.flush()
	})

	t.Run("chain order", func(t *testing.T) {
		var order []string

		mw := func(name string) Middleware {
			return func(next Handler) Handler {
				return func(ev entity.Event) error {
					order = append(order, name)
					return next(ev)
				}
			}
		}

		s, _ := newSink(t, 5, mw("first"), mw("second"), mw("third"))
		s.Append(event("temp", 1, 1000))

		assert.Equal(t, []string{"first", "second", "third"}, order)
	})
}
