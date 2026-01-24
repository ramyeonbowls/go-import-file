package importer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type SPErrorResult struct {
	ErrorNumber  int
	ErrorState   int
	ErrorLine    int
	ErrorMessage string
}

func RunSDealFromDummy(
	ctx context.Context,
	dbConn *sql.DB,
	processID string,
) error {

	start := time.Now()
	log.Println("Import SDEAL FROM DUMMY started")

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `
	EXEC dbo.SP_SDEAL_FROM_DUMMY @PROCESS_ID = @PROCESS_ID
	`

	var result struct {
		ErrorNumber  sql.NullInt64
		ErrorState   sql.NullInt64
		ErrorLine    sql.NullInt64
		ErrorMessage sql.NullString
	}

	err = tx.QueryRowContext(
		ctx,
		query,
		sql.Named("PROCESS_ID", processID),
	).Scan(
		&result.ErrorNumber,
		&result.ErrorState,
		&result.ErrorLine,
		&result.ErrorMessage,
	)

	if err != nil {
		return fmt.Errorf("execute SP failed: %w", err)
	}

	// Normalize NULL → 0 / ""
	errNum := int64(0)
	if result.ErrorNumber.Valid {
		errNum = result.ErrorNumber.Int64
	}

	errMsg := ""
	if result.ErrorMessage.Valid {
		errMsg = result.ErrorMessage.String
	}

	// Jika ada error
	if errNum != 0 {
		return fmt.Errorf(
			"SP_SDEAL_FROM_DUMMY failed | number=%d state=%v line=%v msg=%s",
			errNum,
			result.ErrorState,
			result.ErrorLine,
			errMsg,
		)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf(
		"SDEAL FROM DUMMY completed | duration=%s",
		time.Since(start),
	)

	return nil
}
