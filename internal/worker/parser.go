package worker

import (
	"bufio"
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-import-file/internal/config"
	"go-import-file/internal/metrics"
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
)

func ParseWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan FileJob,
	fileMetrics chan<- metrics.FileMetric,
	processID string,
	ch16 chan<- model.Mprice,
	ch15 chan<- model.MpriceGrp,
	ch01 chan<- model.Mcust,
	ch25 chan<- model.Msku,
	ch02 chan<- model.McustGrp,
	ch05 chan<- model.McustIndus,
	ch20 chan<- model.Msalesman,
	ch43 chan<- model.SlsInv,
	ch35 chan<- model.ArInvoice,
	ch39 chan<- model.ImStkbal,
	ch108 chan<- model.MBackOrder,
	ch103 chan<- model.Mbeat,
	ch44 chan<- model.McustCl,
	ch112 chan<- model.McustInvD,
	ch111 chan<- model.McustInvH,
	ch03 chan<- model.McustType,
) {
	defer wg.Done()

	handlers := BuildBlockHandlers(ch16, ch15, ch01, ch25, ch02, ch05, ch20, ch43, ch35, ch39, ch108, ch103, ch44, ch112, ch111, ch03)
	cfg := config.Load()

	for job := range jobs {
		err := parseOneFile(ctx, job, fileMetrics, processID, handlers)

		if err != nil {
			utils.MoveFile(job.FilePath, cfg.FileFailedDir)
			continue
		}
		utils.MoveFile(job.FilePath, cfg.FileSuccessDir)
	}
}

func parseOneFile(
	ctx context.Context,
	job FileJob,
	fileMetrics chan<- metrics.FileMetric,
	processID string,
	handlers map[string]BlockHandler,
) error {
	start := time.Now()

	var (
		totalLines int64
		parsedRows int64
		errCount   int64
	)

	file, err := os.Open(job.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 20*1024*1024)

	lineNumber := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lineNumber++
		atomic.AddInt64(&totalLines, 1)
		atomic.AddInt64(&metrics.ProcessedLines, 1)

		fields := strings.Split(scanner.Text(), "|")
		if len(fields) < 2 {
			errCount++
			continue
		}

		blockID := strings.TrimSpace(fields[0])
		handler, ok := handlers[blockID]
		if !ok {
			continue // unknown block → skip
		}

		if err := handler.Handle(fields, lineNumber, job, processID); err != nil {
			errCount++
			continue
		}

		parsedRows++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	fileMetrics <- metrics.FileMetric{
		FileName:   job.FileName,
		StartTime:  start,
		EndTime:    time.Now(),
		Duration:   time.Since(start),
		TotalLines: totalLines,
		ParsedRows: parsedRows,
		ErrorCount: errCount,
		Status:     "SUCCESS",
	}

	return nil
}

/*
=====================================================
 Helper function
=====================================================
*/

func safe(arr []string, idx int) string {
	if idx >= len(arr) {
		return ""
	}
	return strings.TrimSpace(arr[idx])
}
