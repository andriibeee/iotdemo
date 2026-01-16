package transport

import (
	"fmt"

	"github.com/VictoriaMetrics/metrics"
)

var (
	requestsTotal   = metrics.NewCounter("http_requests_total")
	requestDuration = metrics.NewSummary("http_request_duration_seconds")
	requestSize     = metrics.NewSummary("http_request_size_bytes")
	responseSize    = metrics.NewSummary("http_response_size_bytes")
	activeRequests  = metrics.NewGauge("http_active_requests", nil)

	batchTotal       = metrics.NewCounter("http_batch_total")
	batchEventsTotal = metrics.NewCounter("http_batch_events_total")
	batchDropped     = metrics.NewCounter("http_batch_dropped_total")
	batchParseErrors = metrics.NewCounter("http_batch_parse_errors_total")
)

func requestsByPathAndStatus(path string, status int) *metrics.Counter {
	return metrics.GetOrCreateCounter(fmt.Sprintf(`http_requests_total{path=%q,status="%d"}`, path, status))
}
