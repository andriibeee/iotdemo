package sink

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/andriibeee/iotdemo/internal/entity"
	"github.com/andriibeee/iotdemo/pkg/journal"
	"github.com/andriibeee/iotdemo/pkg/rb"
)

var (
	ErrJournalIsNil = errors.New("journal is nil")
	ErrSinkClosed   = errors.New("sink is closed")
)

type Handler func(ev entity.Event) error

type Middleware func(next Handler) Handler

type Option func(*Sink)

func WithBufSize(size int) Option {
	return func(s *Sink) {
		s.bufSize = size
	}
}

func WithMiddleware(middlewares ...Middleware) Option {
	return func(s *Sink) {
		s.middlewares = append(s.middlewares, middlewares...)
	}
}

const defaultBufSize = 128

type Sink struct {
	journal     Journal
	buf         *rb.RingBuffer[entity.Event]
	handler     Handler
	bufSize     int
	middlewares []Middleware
	closed      atomic.Bool
}

func New(j Journal, opts ...Option) *Sink {
	s := &Sink{
		journal: j,
		bufSize: defaultBufSize,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.buf = rb.New[entity.Event](s.bufSize)
	s.handler = s.buildChain(s.middlewares)
	return s
}

func (s *Sink) buildChain(middlewares []Middleware) Handler {
	h := s.appendToBuffer
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

func (s *Sink) appendToBuffer(ev entity.Event) error {
	eventsReceived.Inc()
	loot, isDropped := s.buf.Add(ev)
	eventsBuffered.Inc()
	if isDropped {
		val, err := loot.MarshalMsg(nil)
		if err != nil {
			return err
		}
		if _, err = s.journal.Write(
			s.fmtKey(loot.Sensor, loot.UnixTimestamp),
			val,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) fmtKey(sensor string, ts int64) []byte {
	var b bytes.Buffer
	b.WriteString("sensor_")
	b.WriteString(sensor)
	b.WriteString("{ts=")
	b.WriteString(strconv.FormatInt(ts, 10))
	b.WriteString("}")
	return b.Bytes()
}

func (s *Sink) Append(ev entity.Event) error {
	if s.closed.Load() {
		return ErrSinkClosed
	}
	if s.journal == nil {
		return ErrJournalIsNil
	}
	return s.handler(ev)
}

func (s *Sink) Run(ctx context.Context) error {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			s.closed.Store(true)
			if err := s.flush(); err != nil {
				return err
			}
			return ctx.Err()
		case <-t.C:
			if err := s.flush(); err != nil {
				return err
			}
		}
	}
}

func (s *Sink) flush() error {
	if s.journal == nil {
		return ErrJournalIsNil
	}

	var batch []journal.Entry
	for ev := range s.buf.All() {
		val, err := ev.MarshalMsg(nil)
		if err != nil {
			flushErrors.Inc()
			return err
		}
		batch = append(batch, journal.Entry{
			Key:   s.fmtKey(ev.Sensor, ev.UnixTimestamp),
			Value: val,
		})
	}

	flushTotal.Inc()
	if _, err := s.journal.WriteBatch(batch); err != nil {
		flushErrors.Inc()
		return err
	}
	return nil
}

func (s *Sink) Close() error {
	s.closed.Store(true)
	return s.flush()
}
