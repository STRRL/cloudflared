package proxy

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cloudflare/cloudflared/connection"
	"github.com/cloudflare/cloudflared/ingress"
)

// Metrics uses connection.MetricsNamespace(aka cloudflared) as namespace and connection.TunnelSubsystem
// (tunnel) as subsystem to keep them consistent with the previous qualifier.

var (
	totalRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "total_requests",
			Help:      "Amount of requests proxied through all the tunnels",
		},
	)
	concurrentRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "concurrent_requests_per_tunnel",
			Help:      "Concurrent requests proxied through each tunnel",
		},
	)
	responseByCode = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "response_by_code",
			Help:      "Count of responses by HTTP status code",
		},
		[]string{"status_code"},
	)
	requestErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "request_errors",
			Help:      "Count of error proxying to origin",
		},
	)
	activeTCPSessions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: "tcp",
			Name:      "active_sessions",
			Help:      "Concurrent count of TCP sessions that are being proxied to any origin",
		},
	)
	totalTCPSessions = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: "tcp",
			Name:      "total_sessions",
			Help:      "Total count of TCP sessions that have been proxied to any origin",
		},
	)
	connectLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: "proxy",
			Name:      "connect_latency",
			Help:      "Time it takes to establish and acknowledge connections in milliseconds",
			Buckets:   []float64{1, 10, 25, 50, 100, 500, 1000, 5000},
		},
	)
	connectStreamErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: "proxy",
			Name:      "connect_streams_errors",
			Help:      "Total count of failure to establish and acknowledge connections",
		},
	)
	hostRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_requests_total",
			Help:      "Count of completed requests by ingress route",
		},
		[]string{"host", "path", "method", "status"},
	)
	hostRequestErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_request_errors_total",
			Help:      "Count of proxy request failures by ingress route",
		},
		[]string{"host", "path", "method"},
	)
	hostConnectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_connect_duration_seconds",
			Help:      "Origin connection setup time by ingress route",
		},
		[]string{"host", "path", "method", "status"},
	)
	hostHeaderDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_header_duration_seconds",
			Help:      "Time to receive origin response headers by ingress route",
		},
		[]string{"host", "path", "method", "status"},
	)
	hostResponseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_response_duration_seconds",
			Help:      "Origin response or stream time by ingress route",
		},
		[]string{"host", "path", "method", "status"},
	)
	hostRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_request_duration_seconds",
			Help:      "Completed request time by ingress route",
		},
		[]string{"host", "path", "method", "status"},
	)
	hostRequestBodySize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_request_body_size_bytes",
			Help:      "Request body or stream bytes read by ingress route",
			Buckets:   prometheus.ExponentialBuckets(64, 4, 11),
		},
		[]string{"host", "path", "method", "status"},
	)
	hostResponseBodySize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: connection.MetricsNamespace,
			Subsystem: connection.TunnelSubsystem,
			Name:      "host_response_body_size_bytes",
			Help:      "Response body or stream bytes written by ingress route",
			Buckets:   prometheus.ExponentialBuckets(64, 4, 11),
		},
		[]string{"host", "path", "method", "status"},
	)
)

func init() {
	prometheus.MustRegister(
		totalRequests,
		concurrentRequests,
		responseByCode,
		requestErrors,
		activeTCPSessions,
		totalTCPSessions,
		connectLatency,
		connectStreamErrors,
		hostRequests,
		hostRequestErrors,
		hostConnectDuration,
		hostHeaderDuration,
		hostResponseDuration,
		hostRequestDuration,
		hostRequestBodySize,
		hostResponseBodySize,
	)
}

type hostRequestMetrics struct {
	host          string
	path          string
	method        string
	start         time.Time
	status        atomic.Int64
	requestBytes  atomic.Int64
	responseBytes atomic.Int64
	proxyError    atomic.Bool

	timingMutex      sync.Mutex
	originStart      time.Time
	connectStart     time.Time
	connectDuration  time.Duration
	connectObserved  bool
	connectFinal     bool
	headerDuration   time.Duration
	headerObserved   bool
	responseObserved bool

	streamMutex     sync.Mutex
	streamDone      chan struct{}
	streamRemaining int
}

func newHostRequestMetrics(rule *ingress.Rule, method string, start time.Time) *hostRequestMetrics {
	host := rule.Hostname
	if host == "" {
		host = "*"
	}

	path := "*"
	if rule.Path != nil && rule.Path.Regexp != nil && rule.Path.Regexp.String() != "" {
		path = rule.Path.Regexp.String()
	}

	return &hostRequestMetrics{
		host:   host,
		path:   path,
		method: hostMetricMethod(method),
		start:  start,
	}
}

func hostMetricMethod(method string) string {
	switch method {
	case http.MethodConnect,
		http.MethodDelete,
		http.MethodGet,
		http.MethodHead,
		http.MethodOptions,
		http.MethodPatch,
		http.MethodPost,
		http.MethodPut,
		http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

func (m *hostRequestMetrics) setStatus(status int) {
	for {
		current := m.status.Load()
		if current != 0 && (current < 100 || current >= 200 || current == http.StatusSwitchingProtocols) {
			return
		}
		if m.status.CompareAndSwap(current, int64(status)) {
			return
		}
	}
}

func (m *hostRequestMetrics) statusLabel() string {
	status := m.status.Load()
	if status == 0 {
		return "-"
	}
	return strconv.FormatInt(status, 10)
}

func (m *hostRequestMetrics) startOrigin() {
	m.timingMutex.Lock()
	m.originStart = time.Now()
	m.timingMutex.Unlock()
}

func (m *hostRequestMetrics) startConnect() {
	m.timingMutex.Lock()
	if m.connectStart.IsZero() {
		m.connectStart = time.Now()
	}
	m.timingMutex.Unlock()
}

func (m *hostRequestMetrics) finishConnect(reused bool, final bool) {
	m.timingMutex.Lock()
	defer m.timingMutex.Unlock()

	if m.connectFinal {
		return
	}
	if reused {
		m.connectDuration = 0
		m.connectObserved = true
		m.connectFinal = true
		return
	}
	if m.connectStart.IsZero() {
		return
	}
	m.connectDuration = time.Since(m.connectStart)
	m.connectObserved = true
	m.connectFinal = final
}

func (m *hostRequestMetrics) originTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) {
			m.startConnect()
		},
		ConnectDone: func(_, _ string, _ error) {
			m.finishConnect(false, false)
		},
		GotConn: func(info httptrace.GotConnInfo) {
			m.finishConnect(info.Reused, true)
		},
	}
}

func (m *hostRequestMetrics) receiveOriginHeaders() {
	m.timingMutex.Lock()
	defer m.timingMutex.Unlock()

	m.headerDuration = time.Since(m.originStart)
	m.headerObserved = true
	m.responseObserved = true
}

func (m *hostRequestMetrics) startOriginStream() {
	m.timingMutex.Lock()
	m.responseObserved = true
	m.timingMutex.Unlock()
}

func (m *hostRequestMetrics) markProxyError() {
	m.proxyError.Store(true)
}

func (m *hostRequestMetrics) startStream() {
	m.streamMutex.Lock()
	defer m.streamMutex.Unlock()

	if m.streamDone != nil {
		return
	}
	m.streamDone = make(chan struct{})
	m.streamRemaining = 2
}

func (m *hostRequestMetrics) finishStreamDirection() {
	m.streamMutex.Lock()
	defer m.streamMutex.Unlock()

	if m.streamDone == nil || m.streamRemaining == 0 {
		return
	}
	m.streamRemaining--
	if m.streamRemaining == 0 {
		close(m.streamDone)
	}
}

func (m *hostRequestMetrics) waitForStream() {
	m.streamMutex.Lock()
	done := m.streamDone
	m.streamMutex.Unlock()

	if done != nil {
		<-done
	}
}

func (m *hostRequestMetrics) finish() {
	m.waitForStream()
	status := m.statusLabel()
	labels := []string{m.host, m.path, m.method, status}
	now := time.Now()

	hostRequests.WithLabelValues(labels...).Inc()
	if m.proxyError.Load() {
		hostRequestErrors.WithLabelValues(m.host, m.path, m.method).Inc()
	}
	hostRequestDuration.WithLabelValues(labels...).Observe(now.Sub(m.start).Seconds())
	hostRequestBodySize.WithLabelValues(labels...).Observe(float64(m.requestBytes.Load()))
	hostResponseBodySize.WithLabelValues(labels...).Observe(float64(m.responseBytes.Load()))

	m.timingMutex.Lock()
	defer m.timingMutex.Unlock()

	if m.connectObserved {
		hostConnectDuration.WithLabelValues(labels...).Observe(m.connectDuration.Seconds())
	}
	if m.headerObserved {
		hostHeaderDuration.WithLabelValues(labels...).Observe(m.headerDuration.Seconds())
	}
	if m.responseObserved {
		hostResponseDuration.WithLabelValues(labels...).Observe(now.Sub(m.originStart).Seconds())
	}
}

type hostRequestBody struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (r *hostRequestBody) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

func (r *hostRequestBody) WriteTo(w io.Writer) (int64, error) {
	if writerTo, ok := r.ReadCloser.(io.WriterTo); ok {
		n, err := writerTo.WriteTo(w)
		r.bytes.Add(n)
		return n, err
	}
	return io.Copy(w, struct{ io.Reader }{Reader: r})
}

type hostResponseWriter struct {
	connection.ResponseWriter
	metrics *hostRequestMetrics
}

func newHostResponseWriter(w connection.ResponseWriter, metrics *hostRequestMetrics) connection.ResponseWriter {
	base := &hostResponseWriter{
		ResponseWriter: w,
		metrics:        metrics,
	}
	if flusher, ok := w.(http.Flusher); ok {
		return &hostFlushingResponseWriter{
			hostResponseWriter: base,
			flusher:            flusher,
		}
	}
	return base
}

func (w *hostResponseWriter) Write(p []byte) (int, error) {
	w.metrics.setStatus(http.StatusOK)
	n, err := w.ResponseWriter.Write(p)
	w.metrics.responseBytes.Add(int64(n))
	if err != nil {
		w.metrics.markProxyError()
	}
	return n, err
}

func (w *hostResponseWriter) WriteHeader(status int) {
	w.metrics.setStatus(status)
	w.ResponseWriter.WriteHeader(status)
}

func (w *hostResponseWriter) WriteRespHeaders(status int, header http.Header) error {
	err := w.ResponseWriter.WriteRespHeaders(status, header)
	if err != nil {
		w.metrics.markProxyError()
		return err
	}
	w.metrics.setStatus(status)
	return nil
}

func (w *hostResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, readWriter, err := w.ResponseWriter.Hijack()
	if err != nil {
		w.metrics.markProxyError()
		return nil, nil, err
	}
	if err := readWriter.Flush(); err != nil {
		w.metrics.markProxyError()
		return conn, readWriter, err
	}

	buffered := make([]byte, readWriter.Reader.Buffered())
	if len(buffered) > 0 {
		peeked, err := readWriter.Reader.Peek(len(buffered))
		if err != nil {
			w.metrics.markProxyError()
			return conn, readWriter, err
		}
		copy(buffered, peeked)
	}

	tracked := &hostMetricsConn{
		Conn:    conn,
		metrics: w.metrics,
	}
	reader := io.MultiReader(bytes.NewReader(buffered), conn)
	readWriter.Reader.Reset(&hostMetricsReader{
		Reader:  reader,
		metrics: w.metrics,
	})
	readWriter.Writer.Reset(tracked)
	return tracked, readWriter, nil
}

func (w *hostResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

type hostFlushingResponseWriter struct {
	*hostResponseWriter
	flusher http.Flusher
}

func (w *hostFlushingResponseWriter) Flush() {
	w.flusher.Flush()
}

type hostMetricsReader struct {
	io.Reader
	metrics *hostRequestMetrics
}

func (r *hostMetricsReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.metrics.requestBytes.Add(int64(n))
	if err != nil && err != io.EOF {
		r.metrics.markProxyError()
	}
	return n, err
}

type hostMetricsConn struct {
	net.Conn
	metrics *hostRequestMetrics
}

func (c *hostMetricsConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.metrics.requestBytes.Add(int64(n))
	if err != nil && err != io.EOF {
		c.metrics.markProxyError()
	}
	return n, err
}

func (c *hostMetricsConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.metrics.responseBytes.Add(int64(n))
	if err != nil {
		c.metrics.markProxyError()
	}
	return n, err
}

type hostReadWriteAcker struct {
	connection.ReadWriteAcker
	metrics *hostRequestMetrics
}

func (rw *hostReadWriteAcker) OnStreamError(error) {
	rw.metrics.markProxyError()
}

func (rw *hostReadWriteAcker) OnStreamStart() {
	rw.metrics.startStream()
}

func (rw *hostReadWriteAcker) OnStreamDone() {
	rw.metrics.finishStreamDirection()
}

func incrementRequests() {
	totalRequests.Inc()
	concurrentRequests.Inc()
}

func decrementConcurrentRequests() {
	concurrentRequests.Dec()
}

func incrementTCPRequests() {
	incrementRequests()
	totalTCPSessions.Inc()
	activeTCPSessions.Inc()
}

func decrementTCPConcurrentRequests() {
	decrementConcurrentRequests()
	activeTCPSessions.Dec()
}
