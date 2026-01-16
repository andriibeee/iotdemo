package sink

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/andriibeee/iotdemo/internal/entity"
	apperr "github.com/andriibeee/iotdemo/internal/errors"
)

var (
	dedupTotal   = metrics.NewCounter("sink_dedup_total")
	dedupDropped = metrics.NewCounter("sink_dedup_dropped_total")
)

type Deduplicator struct {
	m        sync.Map
	count    atomic.Uint64
	interval time.Duration
}

func NewDeduplicator(interval time.Duration) *Deduplicator {
	return &Deduplicator{
		interval: interval,
	}
}

func (d *Deduplicator) Start() {
	if d.interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()
		for range ticker.C {
			d.m.Range(func(key, value interface{}) bool {
				d.m.Delete(key)
				return true
			})
			d.count.Store(0)
		}
	}()
}

func (d *Deduplicator) Middleware() Middleware {
	return func(next Handler) Handler {
		return func(ev entity.Event) error {
			if ev.IdempotencyID == "" {
				return next(ev)
			}

			dedupTotal.Inc()

			if _, loaded := d.m.LoadOrStore(ev.IdempotencyID, struct{}{}); loaded {
				dedupDropped.Inc()
				slog.Debug("duplicate event dropped", "idempotency_id", ev.IdempotencyID)
				return apperr.ErrDuplicate
			}

			d.count.Add(1)

			return next(ev)
		}
	}
}

func (d *Deduplicator) Count() uint {
	return uint(d.count.Load())
}
