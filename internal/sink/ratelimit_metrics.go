package sink

import "github.com/VictoriaMetrics/metrics"

var (
	rateLimitAllowed = metrics.NewCounter("ratelimiter_events_allowed_total")
	rateLimitDropped = metrics.NewCounter("ratelimiter_events_dropped_total")
	rateLimitBytes   = metrics.NewCounter("ratelimiter_bytes_total")
)
