//go:build duckmon

package proxy

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	duckmon "github.com/STRRL/duckmon/bindings/go"
)

// The duckmon access log is configured through environment variables.
// TUNNEL_DUCKMON_DIR is the store directory; an empty value disables the
// access log. TUNNEL_DUCKMON_SAMPLE is the share of requests to record,
// from 0 to 1, default 1; requests that failed are always recorded.
// TUNNEL_DUCKMON_KEEP_DAYS is how many days of data to keep, default 7.
const (
	duckmonDirEnv      = "TUNNEL_DUCKMON_DIR"
	duckmonSampleEnv   = "TUNNEL_DUCKMON_SAMPLE"
	duckmonKeepDaysEnv = "TUNNEL_DUCKMON_KEEP_DAYS"

	duckmonDefaultKeepDays = 7
	duckmonCompactInterval = time.Hour
)

var (
	duckmonOnce   sync.Once
	duckmonStore  *duckmon.Store
	duckmonSample float64
	// duckmonDroppedEvents counts events that the store refused, for example
	// because its queue was full. Drops are counted instead of logged.
	duckmonDroppedEvents atomic.Uint64
)

// duckmonInit reads the environment and opens the store. It runs once, on
// the first proxied request. When the directory is not set or the store
// cannot be opened, the access log stays off.
func duckmonInit() {
	dir := os.Getenv(duckmonDirEnv)
	if dir == "" {
		return
	}

	sample := 1.0
	if text := os.Getenv(duckmonSampleEnv); text != "" {
		if value, err := strconv.ParseFloat(text, 64); err == nil && value >= 0 && value <= 1 {
			sample = value
		}
	}

	keepDays := duckmonDefaultKeepDays
	if text := os.Getenv(duckmonKeepDaysEnv); text != "" {
		if value, err := strconv.Atoi(text); err == nil && value > 0 {
			keepDays = value
		}
	}

	store, err := duckmon.Open(dir, duckmon.Options{
		Resource: map[string]any{"service.name": "cloudflared"},
	})
	if err != nil {
		return
	}

	duckmonStore = store
	duckmonSample = sample
	go duckmonCompactLoop(dir, keepDays)
	go duckmonFlushOnShutdown(store)
}

// recordRequestEvent writes one span event for a finished proxied request.
// It is called from hostRequestMetrics.finish with the timing mutex held.
func recordRequestEvent(m *hostRequestMetrics, now time.Time) {
	duckmonOnce.Do(duckmonInit)
	store := duckmonStore
	if store == nil {
		return
	}

	failed := m.proxyError.Load()
	if !failed && duckmonSample < 1 && rand.Float64() >= duckmonSample {
		return
	}

	attrs := map[string]any{
		"host":           m.host,
		"path":           m.path,
		"method":         m.method,
		"status":         int(m.status.Load()),
		"request_bytes":  m.requestBytes.Load(),
		"response_bytes": m.responseBytes.Load(),
		"error":          failed,
	}
	if m.connectObserved {
		attrs["connect_ms"] = durationMillis(m.connectDuration)
	}
	if m.headerObserved {
		attrs["header_ms"] = durationMillis(m.headerDuration)
	}
	if m.responseObserved {
		attrs["response_ms"] = durationMillis(now.Sub(m.originStart))
	}

	duration := now.Sub(m.start)
	if duration < 0 {
		duration = 0
	}

	err := store.Write(duckmon.Event{
		Name:     "http.request",
		Time:     m.start,
		Duration: &duration,
		TraceID:  fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64()),
		SpanID:   fmt.Sprintf("%016x", rand.Uint64()),
		Attrs:    attrs,
	})
	if err != nil {
		duckmonDroppedEvents.Add(1)
	}
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// duckmonCompactLoop merges small files and deletes old days once an hour.
func duckmonCompactLoop(dir string, keepDays int) {
	for {
		time.Sleep(duckmonCompactInterval)
		duckmonCompact(dir, keepDays)
	}
}

func duckmonCompact(dir string, keepDays int) {
	defer func() {
		_ = recover()
	}()
	keep := keepDays
	_, _ = duckmon.Compact(dir, duckmon.CompactOptions{KeepDays: &keep})
}

// duckmonFlushOnShutdown flushes buffered events once the process is asked
// to stop, so the last requests reach a Parquet file before exit. The store
// stays open, so requests that finish during shutdown are still recorded.
func duckmonFlushOnShutdown(store *duckmon.Store) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	signal.Stop(signals)
	_ = store.Flush()
}
