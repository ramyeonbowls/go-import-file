// internal/metrics/progress.go
package metrics

import "sync/atomic"

var (
	TotalLines     int64
	ProcessedLines int64
	InsertedRows   int64
)

func IncProcessed(n int64) {
	atomic.AddInt64(&ProcessedLines, n)
}
