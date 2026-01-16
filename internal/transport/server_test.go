package transport

import (
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	"github.com/andriibeee/iotdemo/internal/entity"
	apperr "github.com/andriibeee/iotdemo/internal/errors"
)

type mockSink struct {
	events []entity.Event
	err    error
}

func (m *mockSink) Append(ev entity.Event) error {
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, ev)
	return nil
}

func newEventRequest(body []byte) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/ingest")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.Header.SetContentType("application/msgpack")
	if body != nil {
		ctx.Request.SetBody(body)
	}
	return ctx
}

func sampleEvent() (entity.Event, []byte) {
	ev := entity.Event{Sensor: "temp", Value: 42, UnixTimestamp: 1000}
	body, _ := ev.MarshalMsg(nil)
	return ev, body
}

func TestHandleEvent(t *testing.T) {
	t.Run("valid event gets accepted", func(t *testing.T) {
		sink := &mockSink{}
		srv := New(sink)
		_, body := sampleEvent()

		ctx := newEventRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusAccepted, ctx.Response.StatusCode())
		assert.Len(t, sink.events, 1)
	})

	t.Run("empty body", func(t *testing.T) {
		srv := New(&mockSink{})
		ctx := newEventRequest(nil)

		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	})

	t.Run("garbage msgpack", func(t *testing.T) {
		srv := New(&mockSink{})
		ctx := newEventRequest([]byte("nope"))

		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	})

	t.Run("sink failure returns 500", func(t *testing.T) {
		srv := New(&mockSink{err: errors.New("db down")})
		_, body := sampleEvent()

		ctx := newEventRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
	})
}

func TestServerIntegration(t *testing.T) {
	sink := &mockSink{}
	srv := New(sink)

	ln := fasthttputil.NewInmemoryListener()
	srv.srv.Handler = srv.handle

	go func() { srv.srv.Serve(ln) }()
	defer ln.Close()

	client := &fasthttp.Client{
		Dial: func(_ string) (net.Conn, error) { return ln.Dial() },
	}

	_, body := sampleEvent()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI("http://test/ingest")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/msgpack")
	req.SetBody(body)

	require.NoError(t, client.Do(req, resp))
	assert.Equal(t, fasthttp.StatusAccepted, resp.StatusCode())
}

func postEvent(t *testing.T, client *fasthttp.Client, addr string) *fasthttp.Response {
	t.Helper()

	_, body := sampleEvent()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	t.Cleanup(func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	})

	req.SetRequestURI("https://" + addr + "/ingest")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/msgpack")
	req.SetBody(body)

	require.NoError(t, client.Do(req, resp))
	return resp
}

func newBatchRequest(body string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/ingest/batch")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.Header.SetContentType("application/x-ndjson")
	ctx.Request.SetBodyString(body)
	return ctx
}

func TestHandleBatch(t *testing.T) {
	t.Run("accepts valid jsonl", func(t *testing.T) {
		sink := &mockSink{}
		srv := New(sink)

		body := `{"sensor":"temp","val":10,"ts":1000}
{"sensor":"temp","val":20,"ts":2000}
{"sensor":"humidity","val":65,"ts":3000}`

		ctx := newBatchRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusAccepted, ctx.Response.StatusCode())
		assert.Len(t, sink.events, 3)
	})

	t.Run("skips empty lines", func(t *testing.T) {
		sink := &mockSink{}
		srv := New(sink)

		body := `{"sensor":"temp","val":10,"ts":1000}

{"sensor":"temp","val":20,"ts":2000}`

		ctx := newBatchRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusAccepted, ctx.Response.StatusCode())
		assert.Len(t, sink.events, 2)
	})

	t.Run("rejects wrong content type", func(t *testing.T) {
		srv := New(&mockSink{})

		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI("/ingest/batch")
		ctx.Request.Header.SetMethod("POST")
		ctx.Request.Header.SetContentType("application/json")
		ctx.Request.SetBodyString(`{}`)

		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusUnsupportedMediaType, ctx.Response.StatusCode())
	})

	t.Run("drops batch on parse error", func(t *testing.T) {
		sink := &mockSink{}
		srv := New(sink)

		body := `{"sensor":"temp","val":10,"ts":1000}
not json
{"sensor":"temp","val":20,"ts":2000}`

		ctx := newBatchRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
		assert.Empty(t, sink.events)
	})

	t.Run("drops remaining on rate limit", func(t *testing.T) {
		calls := 0
		ms := &mockSink{}
		ms.err = nil

		srv := New(&rateLimitAfterN{n: 2, sink: ms})

		body := `{"sensor":"temp","val":1,"ts":1000}
{"sensor":"temp","val":2,"ts":2000}
{"sensor":"temp","val":3,"ts":3000}
{"sensor":"temp","val":4,"ts":4000}`

		ctx := newBatchRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusTooManyRequests, ctx.Response.StatusCode())
		assert.Len(t, ms.events, 2)
		_ = calls
	})

	t.Run("drops remaining on sink error", func(t *testing.T) {
		ms := &errorAfterN{n: 1, err: errors.New("boom")}
		srv := New(ms)

		body := `{"sensor":"temp","val":1,"ts":1000}
{"sensor":"temp","val":2,"ts":2000}`

		ctx := newBatchRequest(body)
		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
		assert.Len(t, ms.events, 1)
	})

	t.Run("empty body", func(t *testing.T) {
		srv := New(&mockSink{})

		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI("/ingest/batch")
		ctx.Request.Header.SetMethod("POST")
		ctx.Request.Header.SetContentType("application/x-ndjson")

		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	})

	t.Run("accepts application/jsonl", func(t *testing.T) {
		sink := &mockSink{}
		srv := New(sink)

		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI("/ingest/batch")
		ctx.Request.Header.SetMethod("POST")
		ctx.Request.Header.SetContentType("application/jsonl")
		ctx.Request.SetBodyString(`{"sensor":"temp","val":10,"ts":1000}`)

		srv.handle(ctx)

		assert.Equal(t, fasthttp.StatusAccepted, ctx.Response.StatusCode())
	})
}

type rateLimitAfterN struct {
	n      int
	count  int
	sink   *mockSink
}

func (r *rateLimitAfterN) Append(ev entity.Event) error {
	if r.count >= r.n {
		return apperr.ErrRateLimited
	}
	r.count++
	return r.sink.Append(ev)
}

type errorAfterN struct {
	n      int
	count  int
	err    error
	events []entity.Event
}

func (e *errorAfterN) Append(ev entity.Event) error {
	if e.count >= e.n {
		return e.err
	}
	e.count++
	e.events = append(e.events, ev)
	return nil
}

func TestBatchIntegration(t *testing.T) {
	sink := &mockSink{}
	srv := New(sink)

	ln := fasthttputil.NewInmemoryListener()
	srv.srv.Handler = srv.handle

	go func() { srv.srv.Serve(ln) }()
	defer ln.Close()

	client := &fasthttp.Client{
		Dial: func(_ string) (net.Conn, error) { return ln.Dial() },
	}

	lines := []string{
		`{"sensor":"s1","val":1,"ts":1}`,
		`{"sensor":"s2","val":2,"ts":2}`,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI("http://test/ingest/batch")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/x-ndjson")
	req.SetBodyString(strings.Join(lines, "\n"))

	require.NoError(t, client.Do(req, resp))
	assert.Equal(t, fasthttp.StatusAccepted, resp.StatusCode())
	assert.Len(t, sink.events, 2)
}
