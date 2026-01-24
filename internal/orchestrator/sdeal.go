package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"go-import-file/internal/config"
	"go-import-file/internal/importer"
	"go-import-file/internal/metrics"
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"go-import-file/internal/worker"
)

func RunSalesDeal(
	ctx context.Context,
	dbConn *sql.DB,
	filePath string,
	processID string,
) error {

	cfg := config.Load()

	files, err := filepath.Glob(filePath + "/SDEAL_*.txt")
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

	log.Printf("TOTAL LINES (SDEAL): %d\n", totalLines)

	// ======================
	// Channels
	// ======================
	jobs := make(chan worker.FileJob, len(files))

	ch120 := make(chan model.SpProsesDpZdhdr, cfg.BufferSize)
	ch121 := make(chan model.SpProsesDpZditm, cfg.BufferSize)
	ch122 := make(chan model.SpProsesDpZddet, cfg.BufferSize)
	ch123 := make(chan model.SpProsesDpZpmix, cfg.BufferSize)
	ch123Promo := make(chan model.SpProsesDpZpmix, cfg.BufferSize)
	ch124 := make(chan model.SpProsesDpZscreg, cfg.BufferSize)
	ch125 := make(chan model.SpProsesDpZscmix, cfg.BufferSize)
	ch126 := make(chan model.SpProsesDpZ00001, cfg.BufferSize)
	ch130 := make(chan model.SpProsesFgZdhdr, cfg.BufferSize)
	ch130Promo := make(chan model.SpProsesFgZdhdr, cfg.BufferSize)
	ch131 := make(chan model.SpProsesFgZfrdet, cfg.BufferSize)
	ch132 := make(chan model.SpProsesFgZfrmix, cfg.BufferSize)

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
	workerCount := runtime.NumCPU() * 2

	log.Printf("Parse Worker Count: %d\n", workerCount)

	var parseWg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
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
			ch120,
			ch121,
			ch122,
			ch123,
			ch123Promo,
			ch124,
			ch125,
			ch126,
			ch130,
			ch130Promo,
			ch131,
			ch132,
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
	done120 := make(chan struct{})
	go worker.Bulk120(ctx, dbConn, ch120, done120)

	done121 := make(chan struct{})
	go worker.Bulk121(ctx, dbConn, ch121, done121)

	done122 := make(chan struct{})
	go worker.Bulk122(ctx, dbConn, ch122, done122)

	done123 := make(chan struct{})
	go worker.Bulk123(ctx, dbConn, ch123, done123)

	done123Promo := make(chan struct{})
	go worker.Bulk123Promo(ctx, dbConn, ch123Promo, done123Promo)

	done124 := make(chan struct{})
	go worker.Bulk124(ctx, dbConn, ch124, done124)

	done125 := make(chan struct{})
	go worker.Bulk125(ctx, dbConn, ch125, done125)

	done126 := make(chan struct{})
	go worker.Bulk126(ctx, dbConn, ch126, done126)

	done130 := make(chan struct{})
	go worker.Bulk130(ctx, dbConn, ch130, done130)

	done130Promo := make(chan struct{})
	go worker.Bulk130Promo(ctx, dbConn, ch130Promo, done130Promo)

	done131 := make(chan struct{})
	go worker.Bulk131(ctx, dbConn, ch131, done131)

	done132 := make(chan struct{})
	go worker.Bulk132(ctx, dbConn, ch132, done132)

	// ======================
	// Shutdown Order (CRITICAL)
	// ======================
	parseWg.Wait()
	close(ch120)
	<-done120

	close(ch121)
	<-done121

	close(ch122)
	<-done122

	close(ch123)
	<-done123

	close(ch123Promo)
	<-done123Promo

	close(ch124)
	<-done124

	close(ch125)
	<-done125

	close(ch126)
	<-done126

	close(ch130)
	<-done130

	close(ch130Promo)
	<-done130Promo

	close(ch131)
	<-done131

	close(ch132)
	<-done132

	close(fileMetrics)
	<-metricsDone

	close(progressDone)

	log.Printf("SDEAL rows inserted: %d\n",
		atomic.LoadInt64(&metrics.InsertedRows),
	)

	return nil
}

func RunSalesDealTruncate(
	ctx context.Context,
	db *sql.DB,
	processID string,
) error {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.ExecContext(
		ctx,
		`
		TRUNCATE TABLE dbo.DP_FG_CHECK;
		TRUNCATE TABLE dbo.DP_ZDHDR;
		TRUNCATE TABLE dbo.DP_ZDITM;
		TRUNCATE TABLE dbo.DP_ZDDET;
		TRUNCATE TABLE dbo.DP_ZPMIX;
		TRUNCATE TABLE dbo.DP_ZSCREG;
		TRUNCATE TABLE dbo.DP_ZSCMIX;
		TRUNCATE TABLE dbo.FG_ZDHDR;
		TRUNCATE TABLE dbo.FG_ZFRDET;
		TRUNCATE TABLE dbo.FG_ZFRMIX;
		`,
	)

	if err != nil {
		return err
	}

	return tx.Commit()
}

func RunSalesDealFinalizeIdempotent(
	ctx context.Context,
	db *sql.DB,
	processID string,
) error {

	const block = "SDEAL"

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
	if err := importer.RunSDealFromDummy(ctx, db, processID); err != nil {
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
