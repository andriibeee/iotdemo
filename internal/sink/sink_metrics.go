package sink

import "github.com/VictoriaMetrics/metrics"

var (
	eventsReceived = metrics.NewCounter("sink_events_received_total")
	eventsBuffered = metrics.NewCounter("sink_events_buffered_total")
	flushTotal     = metrics.NewCounter("sink_flush_total")
	flushErrors    = metrics.NewCounter("sink_flush_errors_total")
)
