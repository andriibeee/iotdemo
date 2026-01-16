package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fasthttp"

	"github.com/andriibeee/iotdemo/internal/entity"
	apperr "github.com/andriibeee/iotdemo/internal/errors"
)

var ErrNilSink = errors.New("sink is nil")

type TLSConfig struct {
	CertFile string
	KeyFile  string
	ClientCA string
}

type Server struct {
	srv  *fasthttp.Server
	sink Sink
	addr string
	tls  *TLSConfig
}

type Option func(*Server)

func WithAddr(addr string) Option {
	return func(s *Server) { s.addr = addr }
}

func WithReadTimeout(d time.Duration) Option {
	return func(s *Server) { s.srv.ReadTimeout = d }
}

func WithWriteTimeout(d time.Duration) Option {
	return func(s *Server) { s.srv.WriteTimeout = d }
}

func WithTLS(cert, key string) Option {
	return func(s *Server) {
		if s.tls == nil {
			s.tls = &TLSConfig{}
		}
		s.tls.CertFile = cert
		s.tls.KeyFile = key
	}
}

func WithClientCA(ca string) Option {
	return func(s *Server) {
		if s.tls == nil {
			s.tls = &TLSConfig{}
		}
		s.tls.ClientCA = ca
	}
}

func New(sink Sink, opts ...Option) *Server {
	s := &Server{
		sink: sink,
		addr: ":8080",
		srv:  &fasthttp.Server{},
	}
	for _, opt := range opts {
		opt(s)
	}
	s.srv.Handler = s.handle
	return s
}

func (s *Server) handle(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	path := string(ctx.Path())

	requestsTotal.Inc()
	activeRequests.Inc()
	defer activeRequests.Dec()

	requestSize.Update(float64(len(ctx.Request.Body())))

	if s.sink == nil {
		slog.Error("sink not configured")
		ctx.Error(ErrNilSink.Error(), fasthttp.StatusInternalServerError)
		s.recordMetrics(path, fasthttp.StatusInternalServerError, start, ctx)
		return
	}

	switch path {
	case "/ingest":
		s.handleEvent(ctx)
	case "/ingest/batch":
		s.handleBatch(ctx)
	case "/healthz":
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBodyString("ok")
	case "/metrics":
		ctx.SetContentType("text/plain; charset=utf-8")
		metrics.WritePrometheus(ctx, true)
	default:
		ctx.Error("not found", fasthttp.StatusNotFound)
	}

	s.recordMetrics(path, ctx.Response.StatusCode(), start, ctx)
}

func (s *Server) recordMetrics(path string, status int, start time.Time, ctx *fasthttp.RequestCtx) {
	requestsByPathAndStatus(path, status).Inc()
	requestDuration.UpdateDuration(start)
	responseSize.Update(float64(len(ctx.Response.Body())))
}

func (s *Server) handleEvent(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	ct := ctx.Request.Header.ContentType()

	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.Error("empty body", fasthttp.StatusBadRequest)
		return
	}

	var ev entity.Event
	switch {
	case bytes.Equal(ct, []byte("application/json")):
		if err := json.Unmarshal(body, &ev); err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
	case bytes.Equal(ct, []byte("application/msgpack")):
		if _, err := ev.UnmarshalMsg(body); err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
	default:
		ctx.Error("unsupported content-type", fasthttp.StatusUnsupportedMediaType)
		return
	}

	if err := s.sink.Append(ev); err != nil {
		switch {
		case errors.Is(err, apperr.ErrRateLimited):
			ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
		case errors.Is(err, apperr.ErrDuplicate):
			ctx.SetStatusCode(fasthttp.StatusConflict)
		default:
			slog.Error("sink.Append failed", "error", err, "sensor", ev.Sensor)
			ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		}
		return
	}

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func (s *Server) handleBatch(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	ct := string(ctx.Request.Header.ContentType())
	if ct != "application/x-ndjson" && ct != "application/jsonl" {
		ctx.Error("use application/x-ndjson or application/jsonl", fasthttp.StatusUnsupportedMediaType)
		return
	}

	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.Error("empty body", fasthttp.StatusBadRequest)
		return
	}

	batchTotal.Inc()

	var events []entity.Event
	scanner := bufio.NewScanner(bytes.NewReader(body))
	line := 0
	for scanner.Scan() {
		line++
		data := scanner.Bytes()
		if len(data) == 0 {
			continue
		}

		var ev entity.Event
		if err := json.Unmarshal(data, &ev); err != nil {
			batchParseErrors.Inc()
			batchDropped.Inc()
			slog.Warn("batch parse error, dropping batch",
				"line", line,
				"error", err,
				"events_parsed", len(events),
			)
			ctx.Error("parse error at line "+strconv.Itoa(line), fasthttp.StatusBadRequest)
			return
		}
		events = append(events, ev)
	}

	if err := scanner.Err(); err != nil {
		batchParseErrors.Inc()
		batchDropped.Inc()
		slog.Warn("batch scan error", "error", err)
		ctx.Error("scan error", fasthttp.StatusBadRequest)
		return
	}

	batchEventsTotal.Add(len(events))
	slog.Debug("processing batch", "events", len(events), "bytes", len(body))

	for i, ev := range events {
		if err := s.sink.Append(ev); err != nil {
			if errors.Is(err, apperr.ErrDuplicate) {
				continue // skip duplicates in batch
			}

			batchDropped.Inc()

			if errors.Is(err, apperr.ErrRateLimited) {
				slog.Warn("batch rate limited, dropping remaining",
					"processed", i,
					"dropped", len(events)-i,
				)
				ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
				return
			}

			slog.Error("batch sink error, dropping remaining",
				"processed", i,
				"dropped", len(events)-i,
				"error", err,
			)
			ctx.Error("sink error", fasthttp.StatusInternalServerError)
			return
		}
	}

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func (s *Server) Run(ctx context.Context) error {
	if s.tls != nil && s.tls.CertFile != "" {
		slog.Info("starting https server", "addr", s.addr)
	} else {
		slog.Info("starting http server", "addr", s.addr)
	}

	errc := make(chan error, 1)
	go func() {
		if s.tls != nil && s.tls.CertFile != "" {
			errc <- s.serveTLS()
		} else {
			errc <- s.srv.ListenAndServe(s.addr)
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down server")
		if err := s.srv.Shutdown(); err != nil {
			slog.Warn("shutdown error", "error", err)
		}
		return ctx.Err()
	case err := <-errc:
		return err
	}
}

func (s *Server) serveTLS() error {
	slog.Debug("loading tls cert", "cert", s.tls.CertFile, "key", s.tls.KeyFile)

	cert, err := tls.LoadX509KeyPair(s.tls.CertFile, s.tls.KeyFile)
	if err != nil {
		slog.Error("failed to load tls keypair", "error", err)
		return err
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if s.tls.ClientCA != "" {
		slog.Debug("loading client ca", "path", s.tls.ClientCA)
		pem, err := os.ReadFile(s.tls.ClientCA)
		if err != nil {
			slog.Error("failed to read client ca", "error", err)
			return err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		slog.Info("mtls enabled")
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.srv.Serve(tls.NewListener(ln, cfg))
}

func (s *Server) Addr() string { return s.addr }
