// internal/metrics/progress_bar.go
package metrics

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

func StartProgressBar(total int64, done <-chan struct{}) {
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			current := atomic.LoadInt64(&ProcessedLines)
			render(current, total, start)
			fmt.Println()
			return
		case <-ticker.C:
			current := atomic.LoadInt64(&ProcessedLines)
			render(current, total, start)
		}
	}
}

func render(current, total int64, start time.Time) {
	if total <= 0 {
		return
	}

	if current > total {
		current = total
	}

	percent := float64(current) / float64(total)

	barWidth := 30
	filled := min(int(percent*float64(barWidth)), barWidth)

	bar := strings.Repeat("#", filled) +
		strings.Repeat("-", barWidth-filled)

	elapsed := time.Since(start).Seconds()
	speed := float64(current) / elapsed

	fmt.Printf(
		"\r[%s] %6.2f%% | %d/%d rows | %.0f rows/s | %s",
		bar,
		percent*100,
		current,
		total,
		speed,
		time.Since(start).Truncate(time.Second),
	)
}
