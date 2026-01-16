package sink

import (
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	apperr "github.com/andriibeee/iotdemo/internal/errors"
	"github.com/andriibeee/iotdemo/internal/entity"
)

type RateLimiter struct {
	limiter        *rate.Limiter
	DroppedCounter atomic.Uint64
}

func NewRateLimiter(bytesPerSec float64) *RateLimiter {
	return &RateLimiter{limiter: rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))}
}

func (rl *RateLimiter) Middleware() Middleware {
	return func(next Handler) Handler {
		return func(ev entity.Event) error {
			n := ev.Msgsize()
			if !rl.limiter.AllowN(time.Now(), n) {
				rl.DroppedCounter.Add(1)
				rateLimitDropped.Inc()
				return apperr.ErrRateLimited
			}
			rateLimitAllowed.Inc()
			rateLimitBytes.Add(n)
			return next(ev)
		}
	}
}
