//go:build !duckmon

package proxy

import "time"

// recordRequestEvent does nothing in builds without the duckmon tag.
func recordRequestEvent(_ *hostRequestMetrics, _ time.Time) {
}
