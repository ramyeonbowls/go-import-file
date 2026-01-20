package metrics

import "time"

type FileMetric struct {
	FileName   string
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
	TotalLines int64
	ParsedRows int64
	ErrorCount int64
	Status     string
}
