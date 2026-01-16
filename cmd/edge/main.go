package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/lotsa"
	"github.com/valyala/fasthttp"

	"github.com/andriibeee/iotdemo/internal/entity"
	"github.com/andriibeee/iotdemo/pkg/retry"
)

var ErrRateLimited = fmt.Errorf("rate limited")

func main() {
	addr := flag.String("addr", "http://localhost:8080", "sink address")
	sensor := flag.String("sensor", "edge-sensor-1", "sensor name")
	rate := flag.Int("rate", 10, "messages per second")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	workers := flag.Int("workers", 4, "number of concurrent workers")
	flag.Parse()

	if err := run(*addr, *sensor, *rate, *duration, *workers); err != nil {
		slog.Error("simulator failed", "error", err)
		os.Exit(1)
	}
}

func run(addr, sensor string, rate int, duration time.Duration, workers int) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	total := rate * int(duration.Seconds())
	if total == 0 {
		return fmt.Errorf("nothing to send (rate=%d, duration=%s)", rate, duration)
	}

	slog.Info("starting simulator",
		"addr", addr,
		"sensor", sensor,
		"rate", rate,
		"duration", duration,
		"workers", workers,
		"total", total,
	)

	client := &fasthttp.Client{
		MaxConnsPerHost: workers * 2,
	}

	var (
		sent    atomic.Int64
		failed  atomic.Int64
		retried atomic.Int64
	)

	interval := time.Second / time.Duration(rate)
	start := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s, f, r := sent.Load(), failed.Load(), retried.Load()
				slog.Info("progress",
					"sent", s,
					"failed", f,
					"retried", r,
					"elapsed", time.Since(start).Round(time.Second),
				)
			case <-done:
				return
			}
		}
	}()

	lotsa.Ops(total, workers, func(i, _ int) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		targetTime := start.Add(time.Duration(i) * interval)
		if wait := time.Until(targetTime); wait > 0 {
			time.Sleep(wait)
		}

		ev := entity.Event{
			IdempotencyID: uuid.NewString(),
			Sensor:        sensor,
			Value:         i,
			UnixTimestamp: time.Now().UnixMilli(),
		}

		err := sendWithRetry(ctx, client, addr, ev, &retried)
		if err != nil {
			failed.Add(1)
			slog.Debug("send failed", "error", err, "event", i)
		} else {
			sent.Add(1)
		}
	})

	close(done)

	elapsed := time.Since(start)
	actualRate := float64(sent.Load()) / elapsed.Seconds()

	slog.Info("done",
		"sent", sent.Load(),
		"failed", failed.Load(),
		"retried", retried.Load(),
		"elapsed", elapsed.Round(time.Millisecond),
		"actual_rate", fmt.Sprintf("%.1f/s", actualRate),
	)

	return nil
}

func sendWithRetry(ctx context.Context, client *fasthttp.Client, addr string, ev entity.Event, retried *atomic.Int64) error {
	r := retry.New(
		retry.MaxAttempts(3),
		retry.Delay(retry.DelayOptions{
			Delay: 100 * time.Millisecond,
			Func:  retry.DoubleDelay,
			Max:   time.Second,
		}),
	)

	attempt := 0
	return r(ctx, func(_ context.Context) error {
		attempt++
		if attempt > 1 {
			retried.Add(1)
		}

		body, err := ev.MarshalMsg(nil)
		if err != nil {
			return fmt.Errorf("%w: marshal: %w", retry.ErrStop, err)
		}

		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		req.SetRequestURI(addr + "/ingest")
		req.Header.SetMethod("POST")
		req.Header.SetContentType("application/msgpack")
		req.SetBody(body)

		if err := client.DoTimeout(req, resp, 5*time.Second); err != nil {
			return fmt.Errorf("request failed: %w", err)
		}

		code := resp.StatusCode()
		switch {
		case code == fasthttp.StatusAccepted:
			return nil
		// fail silently on dupes
		case code == fasthttp.StatusConflict:
			return nil
		// error cases
		case code == fasthttp.StatusTooManyRequests:
			return ErrRateLimited
		case code >= fasthttp.StatusInternalServerError:
			return fmt.Errorf("server error: %d", code)
		default:
			return fmt.Errorf("%w: unexpected status: %d", retry.ErrStop, code)
		}
	})
}
