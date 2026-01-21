package orchestrator

import (
	"context"
	"database/sql"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go-import-file/internal/config"
	"go-import-file/internal/metrics"
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"go-import-file/internal/worker"
)

func RunMmarket(
	ctx context.Context,
	dbConn *sql.DB,
	filePath string,
	processID string,
) error {

	cfg := config.Load()

	files, err := filepath.Glob(filePath + "/*_MMARKET.txt")
	if err != nil || len(files) == 0 {
		return err
	}

	var totalLines int64
	for _, f := range files {
		c, err := utils.CountLines(f)
		if err != nil {
			return err
		}
		totalLines += c
	}

	atomic.StoreInt64(&metrics.TotalLines, totalLines)
	atomic.StoreInt64(&metrics.ProcessedLines, 0)

	log.Printf("TOTAL LINES (MMARKET): %d\n", totalLines)

	// ======================
	// Channels
	// ======================
	jobs := make(chan worker.FileJob, len(files))
	ch105 := make(chan model.Mmarket, cfg.BufferSize)
	fileMetrics := make(chan metrics.FileMetric, 100)

	// ======================
	// Metrics
	// ======================
	metricsDone := make(chan struct{})
	go metrics.CollectFileMetrics(fileMetrics, metricsDone)

	progressDone := make(chan struct{})
	go metrics.StartProgressBar(totalLines, progressDone)

	// ======================
	// Parse Workers
	// ======================
	var parseWg sync.WaitGroup
	for range cfg.Worker {
		parseWg.Add(1)
		go worker.ParseWorker(
			ctx,
			&parseWg,
			jobs,
			fileMetrics,
			processID,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			ch105,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
	}

	for _, path := range files {
		jobs <- worker.FileJob{
			FilePath: path,
			FileName: filepath.Base(path),
		}
	}
	close(jobs)

	// ======================
	// Bulk Insert
	// ======================
	done105 := make(chan struct{})
	go worker.Bulk105(ctx, dbConn, ch105, done105)

	// ======================
	// Shutdown Order (CRITICAL)
	// ======================
	parseWg.Wait()
	close(ch105)
	<-done105

	close(fileMetrics)
	<-metricsDone

	close(progressDone)

	log.Printf("MMARKET rows inserted: %d\n",
		atomic.LoadInt64(&metrics.InsertedRows),
	)

	return nil
}
