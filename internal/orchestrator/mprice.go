package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go-import-file/internal/config"
	"go-import-file/internal/importer"
	"go-import-file/internal/metrics"
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"go-import-file/internal/worker"
)

func RunMPrice(
	ctx context.Context,
	dbConn *sql.DB,
	filePath string,
	processID string,
) error {

	cfg := config.Load()

	files, err := filepath.Glob(filePath + "/*_MPRICE.txt")
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

	log.Printf("TOTAL LINES: %d\n", totalLines)

	// ======================
	// Channels
	// ======================
	jobs := make(chan worker.FileJob, len(files))
	ch16 := make(chan model.Mprice, cfg.BufferSize)
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
			ch16,
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
	done16 := make(chan struct{})
	go worker.Bulk16(ctx, dbConn, ch16, done16)

	// ======================
	// Shutdown Order (CRITICAL)
	// ======================
	parseWg.Wait()
	close(ch16)
	<-done16

	close(fileMetrics)
	<-metricsDone

	close(progressDone)

	log.Printf("MPRICE rows inserted: %d\n",
		atomic.LoadInt64(&metrics.InsertedRows),
	)

	return nil
}

func RunMPriceFinalizeIdempotent(
	ctx context.Context,
	db *sql.DB,
	processID string,
) error {

	const block = "MPRICE"

	// =============================
	// ACQUIRE FINALIZE LOCK
	// =============================
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status string
	err = tx.QueryRowContext(ctx, `
		SELECT status
		FROM import_finalize_log
		WHERE process_id = @pid AND block_code = @blk
	`, sql.Named("pid", processID), sql.Named("blk", block)).Scan(&status)

	if err == nil {
		switch status {
		case "DONE":
			log.Println("MPRICE FINALIZE already DONE, skip")
			return nil
		case "RUNNING":
			return fmt.Errorf("MPRICE FINALIZE already RUNNING")
		}
	} else if err != sql.ErrNoRows {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		MERGE import_finalize_log AS t
		USING (SELECT @pid AS pid, @blk AS blk) s
		ON t.process_id = s.pid AND t.block_code = s.blk
		WHEN MATCHED THEN
			UPDATE SET status='RUNNING',
				started_at=SYSDATETIME(),
				finished_at=NULL,
				error_message=NULL
		WHEN NOT MATCHED THEN
			INSERT (process_id, block_code, status, started_at)
			VALUES (@pid, @blk, 'RUNNING', SYSDATETIME());
	`,
		sql.Named("pid", processID),
		sql.Named("blk", block),
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// =============================
	// ACTUAL FINALIZE (YOUR FUNCTION)
	// =============================
	if err := importer.RunMPriceFinalize(ctx, db, processID); err != nil {
		_, _ = db.ExecContext(ctx, `
			UPDATE import_finalize_log
			SET status='FAILED',
				finished_at=SYSDATETIME(),
				error_message=@err
			WHERE process_id=@pid AND block_code=@blk
		`,
			sql.Named("err", err.Error()),
			sql.Named("pid", processID),
			sql.Named("blk", block),
		)
		return err
	}

	// =============================
	// MARK DONE
	// =============================
	_, err = db.ExecContext(ctx, `
		UPDATE import_finalize_log
		SET status='DONE',
			finished_at=SYSDATETIME()
		WHERE process_id=@pid AND block_code=@blk
	`,
		sql.Named("pid", processID),
		sql.Named("blk", block),
	)

	return err
}
