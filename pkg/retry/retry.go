package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type (
	Func         func(ctx context.Context) error
	Option       func(next Func) Func
	Retry        func(ctx context.Context, fn Func) error
	DelayFunc    func(duration time.Duration) time.Duration
	DelayOptions struct {
		Delay time.Duration
		Func  DelayFunc
		Max   time.Duration
	}
	StopCondition func(error) bool
)

var (
	ErrStop  = errors.New("retry error")
	ErrRetry = errors.New("retry")
)

func New(options ...Option) Retry {
	return NewExplicit(false, options...)
}

func NewExplicit(explicit bool, options ...Option) Retry {
	return func(ctx context.Context, fn Func) error {
		for _, option := range options {
			fn = option(fn)
		}

		var errs []error

		for {
			err := fn(ctx)
			if err == nil {
				return nil
			}

			errs = append(errs, err)

			if explicit && !errors.Is(err, ErrRetry) {
				return errors.Join(errs...)
			}

			if errors.Is(err, ErrStop) {
				return errors.Join(errs...)
			}
		}
	}
}

func MaxAttempts(n uint) Option {
	return func(fn Func) Func {
		var i uint
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err == nil {
				return nil
			}
			i++
			if i >= n {
				err = fmt.Errorf("%w: %d attempts exceeded: %w", ErrStop, n, err)

			}
			return err
		}
	}
}

func Delay(opt DelayOptions) Option {
	return func(fn Func) Func {
		delay := opt.Delay
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err != nil {
				time.Sleep(delay)
				if opt.Func != nil {
					delay = opt.Func(delay)
				}
				if opt.Max != 0 {
					delay = min(delay, opt.Max)
				}
			}
			return err
		}
	}
}

func Timeout(duration time.Duration) Option {
	return func(fn Func) Func {
		start := time.Now()
		return func(ctx context.Context) error {
			if time.Since(start) > duration {
				return fmt.Errorf("%w: timeout %s", ErrStop, duration)
			}
			return fn(ctx)
		}
	}
}

func Exponential(factor int) DelayFunc {
	return func(d time.Duration) time.Duration {
		return d * time.Duration(factor)
	}
}

var (
	DoubleDelay = Exponential(2)
)
