package worker

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"

	"go-import-file/internal/logger"
	"go-import-file/internal/metrics"
	"go-import-file/internal/model"

	mssql "github.com/microsoft/go-mssqldb"
)

type Logger interface {
	Printf(string, ...any)
}

/* =========================
   CORE BULK INSERT
========================= */

func bulkInsert(
	ctx context.Context,
	db *sql.DB,
	table string,
	cols []string,
	data <-chan func() []any,
	done chan<- struct{},
	l Logger,
) {
	defer close(done)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		l.Printf("[BULK][%s] begin tx failed: %v", table, err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(mssql.CopyIn(table, mssql.BulkOptions{}, cols...))
	if err != nil {
		l.Printf("[BULK][%s] prepare failed: %v", table, err)
		return
	}
	defer stmt.Close()

	var (
		rowNum        int64
		localInserted int64
	)

	for rowFn := range data {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rowNum++
		row := rowFn()

		if _, err := stmt.Exec(row...); err != nil {
			l.Printf(
				"[BULK][%s] exec failed at row #%d\nColumns: %v\nValues : %#v\nError  : %v",
				table, rowNum, cols, row, err,
			)
			return
		}

		localInserted++
		if localInserted%1000 == 0 {
			atomic.AddInt64(&metrics.InsertedRows, localInserted)
			localInserted = 0
		}
	}

	if localInserted > 0 {
		atomic.AddInt64(&metrics.InsertedRows, localInserted)
	}

	if _, err := stmt.Exec(); err != nil {
		l.Printf("[BULK][%s] final exec failed: %v", table, err)
		return
	}

	if err := tx.Commit(); err != nil {
		l.Printf("[BULK][%s] commit failed: %v", table, err)
		return
	}

	l.Printf("[BULK][%s] completed successfully, rows=%d", table, rowNum)
}

/* =========================
   BULK UPSERT VIA TEMP TABLE
========================= */

func bulkUpsertViaTempTable(
	ctx context.Context,
	db *sql.DB,
	targetTable string,
	tempTable string,
	cols []string,
	tempTableDDL string,
	joinCondition string,
	updateSetClause string,
	data <-chan func() []any,
	done chan<- struct{},
	l Logger,
) error {
	defer close(done)

	l.Printf("[BULK-UPSERT][%s] START", targetTable)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// === SQL Server safety & performance ===
	if _, err := tx.Exec(`SET XACT_ABORT ON;`); err != nil {
		return err
	}

	if _, err := tx.Exec(tempTableDDL); err != nil {
		return err
	}

	stmt, err := tx.Prepare(mssql.CopyIn(tempTable, mssql.BulkOptions{}, cols...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	var (
		rowNum        int64
		localInserted int64
	)

	for rowFn := range data {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rowNum++
		if _, err := stmt.Exec(rowFn()...); err != nil {
			return err
		}

		localInserted++
		if localInserted%1000 == 0 {
			atomic.AddInt64(&metrics.InsertedRows, localInserted)
			localInserted = 0
		}
	}

	if localInserted > 0 {
		atomic.AddInt64(&metrics.InsertedRows, localInserted)
	}

	if _, err := stmt.Exec(); err != nil {
		return err
	}

	// === Cache immutable SQL parts ===
	insertCols := strings.Join(cols, ", ")
	insertVals := make([]string, len(cols))
	for i, c := range cols {
		insertVals[i] = "src." + c
	}
	insertValsSQL := strings.Join(insertVals, ", ")

	mergeSQL := `
		SET NOCOUNT ON;

		;WITH src AS (
			SELECT DISTINCT
				` + insertCols + `
			FROM ` + tempTable + `
		)
		MERGE ` + targetTable + ` WITH (HOLDLOCK) AS tgt
		USING src
			ON ` + joinCondition + `
		WHEN MATCHED THEN
			UPDATE SET ` + updateSetClause + `
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (` + insertCols + `)
			VALUES (` + insertValsSQL + `);
	`

	if _, err := tx.Exec(mergeSQL); err != nil {
		return err
	}

	if _, err := tx.Exec(`DROP TABLE ` + tempTable); err != nil {
		return err
	}

	return tx.Commit()
}

/* =========================
   BULK UPSERT VIA TEMP TABLE
========================= */

func bulkUpsertViaTempTableRowNumber(
	ctx context.Context,
	db *sql.DB,
	targetTable string,
	tempTable string,
	cols []string,
	tempTableDDL string,
	joinCondition string,
	updateSetClause string,
	partionColumns string,
	data <-chan func() []any,
	done chan<- struct{},
	l Logger,
) error {
	defer close(done)

	l.Printf("[BULK-UPSERT][%s] START", targetTable)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// === SQL Server safety & performance ===
	if _, err := tx.Exec(`SET XACT_ABORT ON;`); err != nil {
		return err
	}

	if _, err := tx.Exec(tempTableDDL); err != nil {
		return err
	}

	stmt, err := tx.Prepare(mssql.CopyIn(tempTable, mssql.BulkOptions{}, cols...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	var (
		rowNum        int64
		localInserted int64
	)

	for rowFn := range data {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rowNum++
		if _, err := stmt.Exec(rowFn()...); err != nil {
			return err
		}

		localInserted++
		if localInserted%1000 == 0 {
			atomic.AddInt64(&metrics.InsertedRows, localInserted)
			localInserted = 0
		}
	}

	if localInserted > 0 {
		atomic.AddInt64(&metrics.InsertedRows, localInserted)
	}

	if _, err := stmt.Exec(); err != nil {
		return err
	}

	// === Cache immutable SQL parts ===
	insertCols := strings.Join(cols, ", ")
	insertVals := make([]string, len(cols))
	for i, c := range cols {
		insertVals[i] = "src." + c
	}
	insertValsSQL := strings.Join(insertVals, ", ")

	mergeSQL := `
		SET NOCOUNT ON;

		;WITH src AS (
			SELECT
				` + insertValsSQL + `
			FROM (
				SELECT DISTINCT
					ROW_NUMBER() OVER (PARTITION BY ` + partionColumns + ` ORDER BY ` + partionColumns + `) AS RowNum,
					` + insertValsSQL + `
				FROM ` + tempTable + ` as src
			) as src
			WHERE src.RowNum = 1
		)
		MERGE ` + targetTable + ` WITH (HOLDLOCK) AS tgt
		USING src
			ON ` + joinCondition + `
		WHEN MATCHED THEN
			UPDATE SET ` + updateSetClause + `
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (` + insertCols + `)
			VALUES (` + insertValsSQL + `);
	`

	if _, err := tx.Exec(mergeSQL); err != nil {
		return err
	}

	if _, err := tx.Exec(`DROP TABLE ` + tempTable); err != nil {
		return err
	}

	return tx.Commit()
}

/* =========================
   PUBLIC BULK WRITERS
========================= */

func Bulk16(ctx context.Context, db *sql.DB, ch <-chan model.Mprice, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk16")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.m_price_dummy",
		[]string{
			"UNIQ_ID", "LINE_NO", "PRICE_CODE", "BRANCH_ID",
			"PCODE", "PRICE_VALUE", "PRICE_UOM", "CBY",
			"CDATE", "MBY", "MDATE", "CORE_FILENAME",
			"CORE_PROCESSDATE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.UniqID, r.LineNo, r.PriceCode, r.BranchID,
				r.Pcode, r.PriceValue, r.PriceUom, r.Cby,
				r.Cdate, r.Mby, r.Mdate, r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk15(ctx context.Context, db *sql.DB, ch <-chan model.MpriceGrp, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk15")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fgharga",
			"#tmp_fgharga",
			[]string{"GHARGA", "KET", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fgharga (
				GHARGA NVARCHAR(255),
				KET NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.GHARGA = src.GHARGA",
			`
			tgt.GHARGA = src.GHARGA,
			tgt.KET = src.KET,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk01][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.PriceCode,
				r.PriceDesc,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk01(ctx context.Context, db *sql.DB, ch <-chan model.Mcust, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk01")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTableRowNumber(
			ctx,
			db,
			"dbo.fcustmst",
			"#tmp_fcustmst",
			[]string{"CUSTNO", "DATA01", "CUSTNAME", "CUSTADD1", "CUSTADD2", "CCITY", "CCONTACT", "CPHONE1", "CFAXNO", "CTERM", "CLIMIT", "FLAGLIMIT", "GDISC", "GRUPOUT", "TYPEOUT", "GHARGA", "FLAGPAY", "FLAGOUT", "RPP", "LSALES", "LDATETRS", "LOKASI", "DISTRIK", "BEAT", "SUBBEAT", "KLASIF", "KINDUS", "KPASAR", "KODECABANG", "LA", "LG", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fcustmst (
				CUSTNO NVARCHAR(255),
				DATA01 NVARCHAR(255),
				CUSTNAME NVARCHAR(255),
				CUSTADD1 NVARCHAR(255),
				CUSTADD2 NVARCHAR(255),
				CCITY NVARCHAR(255),
				CCONTACT NVARCHAR(255),
				CPHONE1 NVARCHAR(255),
				CFAXNO NVARCHAR(255),
				CTERM NVARCHAR(255),
				CLIMIT INT,
				FLAGLIMIT NVARCHAR(255),
				GDISC NVARCHAR(255),
				GRUPOUT NVARCHAR(255),
				TYPEOUT NVARCHAR(255),
				GHARGA NVARCHAR(255),
				FLAGPAY NVARCHAR(255),
				FLAGOUT NVARCHAR(255),
				RPP INT,
				LSALES INT,
				LDATETRS NVARCHAR(255),
				LOKASI NVARCHAR(255),
				DISTRIK NVARCHAR(255),
				BEAT NVARCHAR(255),
				SUBBEAT NVARCHAR(255),
				KLASIF NVARCHAR(255),
				KINDUS NVARCHAR(255),
				KPASAR NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				LA NVARCHAR(255),
				LG NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.CUSTNO = src.CUSTNO AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.CUSTNO = src.CUSTNO,
			tgt.DATA01 = src.DATA01,
			tgt.CUSTNAME = src.CUSTNAME,
			tgt.CUSTADD1 = src.CUSTADD1,
			tgt.CUSTADD2 = src.CUSTADD2,
			tgt.CCITY = src.CCITY,
			tgt.CCONTACT = src.CCONTACT,
			tgt.CPHONE1 = src.CPHONE1,
			tgt.CFAXNO = src.CFAXNO,
			tgt.CTERM = src.CTERM,
			tgt.CLIMIT = src.CLIMIT,
			tgt.FLAGLIMIT = src.FLAGLIMIT,
			tgt.GDISC = src.GDISC,
			tgt.GRUPOUT = src.GRUPOUT,
			tgt.TYPEOUT = src.TYPEOUT,
			tgt.GHARGA = src.GHARGA,
			tgt.FLAGPAY = src.FLAGPAY,
			tgt.FLAGOUT = src.FLAGOUT,
			tgt.RPP = src.RPP,
			tgt.LSALES = src.LSALES,
			tgt.LDATETRS = src.LDATETRS,
			tgt.LOKASI = src.LOKASI,
			tgt.DISTRIK = src.DISTRIK,
			tgt.BEAT = src.BEAT,
			tgt.SUBBEAT = src.SUBBEAT,
			tgt.KLASIF = src.KLASIF,
			tgt.KINDUS = src.KINDUS,
			tgt.KPASAR = src.KPASAR,
			tgt.KODECABANG = src.KODECABANG,
			tgt.LA = src.LA,
			tgt.LG = src.LG,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			"src.CUSTNO, src.KODECABANG",
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk01][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Custno,
				r.Data01,
				r.CustName,
				r.CustAdd1,
				r.CustAdd2,
				r.City,
				r.Contact,
				r.Phone1,
				r.FaxNo,
				r.Cterm,
				r.Climit,
				r.FlagLimit,
				r.Gdisc,
				r.GrupOut,
				r.TypeOut,
				r.Gharga,
				r.FlagPay,
				r.FlagOut,
				r.Rpp,
				r.Lsales,
				r.Ldatetrs,
				r.Lokasi,
				r.Distrik,
				r.Beat,
				r.SubBeat,
				r.Klasif,
				r.Kindus,
				r.Kpasar,
				r.BranchID,
				r.La,
				r.Lg,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}

	close(rows)
}

func Bulk25(ctx context.Context, db *sql.DB, ch <-chan model.Msku, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk25")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fmaster",
			"#tmp_fmaster",
			[]string{
				"PRLIN",
				"BRAND",
				"PCODE",
				"DATA1",
				"PCODENAME",
				"UNIT1",
				"UNIT2",
				"UNIT3",
				"UNIT4",
				"UNIT5",
				"CONVUNIT2",
				"CONVUNIT3",
				"CONVUNIT4",
				"CONVUNIT5",
				"PPN",
				"FLAG_AKTIF",
				"FLAG_GIFT",
				"SHORTNAME1",
				"UOM1_BUY",
				"UOM2_BUY",
				"UOM3_BUY",
				"UOM4_BUY",
				"UOM5_BUY",
				"UOM_BASE",
				"UOM_MAIN",
				"CORE_FILENAME",
				"CORE_PROCESSDATE",
			},
			`
			CREATE TABLE #tmp_fmaster (
				PRLIN NVARCHAR(225),
				BRAND NVARCHAR(225),
				PCODE NVARCHAR(225),
				DATA1 NVARCHAR(225),
				PCODENAME NVARCHAR(225),
				UNIT1 NVARCHAR(225),
				UNIT2 NVARCHAR(225),
				UNIT3 NVARCHAR(225),
				UNIT4 NVARCHAR(225),
				UNIT5 NVARCHAR(225),
				CONVUNIT2 INT,
				CONVUNIT3 INT,
				CONVUNIT4 INT,
				CONVUNIT5 INT,
				PPN INT,
				FLAG_AKTIF NVARCHAR(225),
				FLAG_GIFT NVARCHAR(225),
				SHORTNAME1 NVARCHAR(225),
				UOM1_BUY NVARCHAR(225),
				UOM2_BUY NVARCHAR(225),
				UOM3_BUY NVARCHAR(225),
				UOM4_BUY NVARCHAR(225),
				UOM5_BUY NVARCHAR(225),
				UOM_BASE NVARCHAR(225),
				UOM_MAIN NVARCHAR(225),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.PCODE = src.PCODE",
			`
			tgt.PRLIN = src.PRLIN,
			tgt.BRAND = src.BRAND,
			tgt.PCODE = src.PCODE,
			tgt.DATA1 = src.DATA1,
			tgt.PCODENAME = src.PCODENAME,
			tgt.UNIT1 = src.UNIT1,
			tgt.UNIT2 = src.UNIT2,
			tgt.UNIT3 = src.UNIT3,
			tgt.UNIT4 = src.UNIT4,
			tgt.UNIT5 = src.UNIT5,
			tgt.CONVUNIT2 = src.CONVUNIT2,
			tgt.CONVUNIT3 = src.CONVUNIT3,
			tgt.CONVUNIT4 = src.CONVUNIT4,
			tgt.CONVUNIT5 = src.CONVUNIT5,
			tgt.PPN = src.PPN,
			tgt.FLAG_AKTIF = src.FLAG_AKTIF,
			tgt.FLAG_GIFT = src.FLAG_GIFT,
			tgt.SHORTNAME1 = src.SHORTNAME1,
			tgt.UOM1_BUY = src.UOM1_BUY,
			tgt.UOM2_BUY = src.UOM2_BUY,
			tgt.UOM3_BUY = src.UOM3_BUY,
			tgt.UOM4_BUY = src.UOM4_BUY,
			tgt.UOM5_BUY = src.UOM5_BUY,
			tgt.UOM_BASE = src.UOM_BASE,
			tgt.UOM_MAIN = src.UOM_MAIN,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk01][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Prlin,
				r.Brand,
				r.Pcode,
				r.Data1,
				r.PcodeName,
				r.Unit1,
				r.Unit2,
				r.Unit3,
				r.Unit4,
				r.Unit5,
				r.Convunit2,
				r.Convunit3,
				r.Convunit4,
				r.Convunit5,
				r.Ppn,
				r.FlagAktif,
				r.FlagGift,
				r.ShortName1,
				r.Uom1Buy,
				r.Uom2Buy,
				r.Uom3Buy,
				r.Uom4Buy,
				r.Uom5Buy,
				r.UomBase,
				r.UomMain,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk02(ctx context.Context, db *sql.DB, ch <-chan model.McustGrp, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk02")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fgrupout",
			"#tmp_fgrupout",
			[]string{"GROUPOUT", "GROUPNAME", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fgrupout (
				GROUPOUT NVARCHAR(255),
				GROUPNAME NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.GROUPOUT = src.GROUPOUT",
			`
			tgt.GROUPOUT = src.GROUPOUT,
			tgt.GROUPNAME = src.GROUPNAME,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk01][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.GroupOut,
				r.GroupName,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk05(ctx context.Context, db *sql.DB, ch <-chan model.McustIndus, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk05")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.findustri",
			"#tmp_findustri",
			[]string{"INDUSID", "INDUSNAME", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_findustri (
				INDUSID NVARCHAR(255),
				INDUSNAME NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.INDUSID = src.INDUSID",
			`
			tgt.INDUSID = src.INDUSID,
			tgt.INDUSNAME = src.INDUSNAME,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk01][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.IndusId,
				r.IndusName,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk20(ctx context.Context, db *sql.DB, ch <-chan model.Msalesman, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk20")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fsalesman",
			"#tmp_fsalesman",
			[]string{"SLSNO", "SLSNAME", "ALAMAT1", "ALAMAT2", "KOTA", "PENDIDIKAN", "TGLLAHIR", "TGLMASUK", "TGLTRANS", "SLSPASS", "EC1", "ITEM", "KODECABANG", "ATASAN_ID", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fsalesman (
				SLSNO NVARCHAR(255),
				SLSNAME NVARCHAR(255),
				ALAMAT1 NVARCHAR(255),
				ALAMAT2 NVARCHAR(255),
				KOTA NVARCHAR(255),
				PENDIDIKAN NVARCHAR(255),
				TGLLAHIR NVARCHAR(255),
				TGLMASUK NVARCHAR(255),
				TGLTRANS NVARCHAR(255),
				SLSPASS NVARCHAR(255),
				EC1 NVARCHAR(255),
				ITEM NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				ATASAN_ID NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.SLSNO = src.SLSNO AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.SLSNO = src.SLSNO,
			tgt.SLSNAME = src.SLSNAME,
			tgt.ALAMAT1 = src.ALAMAT1,
			tgt.ALAMAT2 = src.ALAMAT2,
			tgt.KOTA = src.KOTA,
			tgt.PENDIDIKAN = src.PENDIDIKAN,
			tgt.TGLLAHIR = src.TGLLAHIR,
			tgt.TGLMASUK = src.TGLMASUK,
			tgt.TGLTRANS = src.TGLTRANS,
			tgt.SLSPASS = src.SLSPASS,
			tgt.EC1 = src.EC1,
			tgt.ITEM = src.ITEM,
			tgt.KODECABANG = src.KODECABANG,
			tgt.ATASAN_ID = src.ATASAN_ID,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk01][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.SlsNo,
				r.SlsName,
				r.Alamat1,
				r.Alamat2,
				r.Kota,
				r.Pendidikan,
				r.TglLahir,
				r.TglMasuk,
				r.TglTrans,
				r.SlsPass,
				r.Ec1,
				r.Item,
				r.Kodecabang,
				r.AtasanId,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk43(ctx context.Context, db *sql.DB, ch <-chan model.SlsInv, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk43")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.sap_web_inv_sfa",
			"#tmp_sap_web_inv_sfa",
			[]string{"SLSNO", "CUSTNO", "SFA_ORDER_NO", "SFA_ORDER_DATE", "ORDERNO", "ORDER_DATE", "INVOICE_NO", "INVOICE_DATE", "PCODE", "QTY", "PRICE", "DISKON", "KODECABANG", "INV_TYPE", "REF_CN", "INVAMOUNT", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_sap_web_inv_sfa (
				SLSNO NVARCHAR(255),
				CUSTNO NVARCHAR(255),
				SFA_ORDER_NO NVARCHAR(255),
				SFA_ORDER_DATE NVARCHAR(255),
				ORDERNO NVARCHAR(255),
				ORDER_DATE NVARCHAR(255),
				INVOICE_NO NVARCHAR(255),
				INVOICE_DATE NVARCHAR(255),
				PCODE NVARCHAR(255),
				QTY INT,
				PRICE FLOAT,
				DISKON FLOAT,
				KODECABANG NVARCHAR(255),
				INV_TYPE NVARCHAR(255),
				REF_CN NVARCHAR(255),
				INVAMOUNT FLOAT,
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.SLSNO = src.SLSNO AND tgt.CUSTNO = src.CUSTNO AND tgt.SFA_ORDER_NO = src.SFA_ORDER_NO AND tgt.ORDERNO = src.ORDERNO AND tgt.INVOICE_NO = src.INVOICE_NO AND tgt.PCODE = src.PCODE AND tgt.KODECABANG = src.KODECABANG AND tgt.INV_TYPE = src.INV_TYPE",
			`
			tgt.SLSNO = src.SLSNO,
			tgt.CUSTNO = src.CUSTNO,
			tgt.SFA_ORDER_NO = src.SFA_ORDER_NO,
			tgt.SFA_ORDER_DATE = src.SFA_ORDER_DATE,
			tgt.ORDERNO = src.ORDERNO,
			tgt.ORDER_DATE = src.ORDER_DATE,
			tgt.INVOICE_NO = src.INVOICE_NO,
			tgt.INVOICE_DATE = src.INVOICE_DATE,
			tgt.PCODE = src.PCODE,
			tgt.QTY = src.QTY,
			tgt.PRICE = src.PRICE,
			tgt.DISKON = src.DISKON,
			tgt.KODECABANG = src.KODECABANG,
			tgt.INV_TYPE = src.INV_TYPE,
			tgt.REF_CN = src.REF_CN,
			tgt.INVAMOUNT = src.INVAMOUNT,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK43][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.SlsNo,
				r.CustNo,
				r.SfaOrderNo,
				r.SfaOrderDate,
				r.OrderNo,
				r.OrderDate,
				r.InvoiceNo,
				r.InvoiceDate,
				r.Pcode,
				r.Qty,
				r.Price,
				r.Diskon,
				r.Kodecabang,
				r.InvType,
				r.RefCn,
				r.Invamount,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk35(ctx context.Context, db *sql.DB, ch <-chan model.ArInvoice, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk35")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fpiutang_temp",
			"#tmp_fpiutang_temp",
			[]string{"CUSTNO", "INVNO", "INVDATE", "DUEDATE", "INVAMOUNT", "AMOUNTPAID", "SLSNO", "KODECABANG", "INV_TYPE", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fpiutang_temp (
				CUSTNO NVARCHAR(255),
				INVNO NVARCHAR(255),
				INVDATE NVARCHAR(255),
				DUEDATE NVARCHAR(255),
				INVAMOUNT NVARCHAR(255),
				AMOUNTPAID NVARCHAR(255),
				SLSNO NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				INV_TYPE NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.CUSTNO = src.CUSTNO AND tgt.INVNO = src.INVNO AND tgt.SLSNO = src.SLSNO AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.CUSTNO = src.CUSTNO,
			tgt.INVNO = src.INVNO,
			tgt.INVDATE = src.INVDATE,
			tgt.DUEDATE = src.DUEDATE,
			tgt.INVAMOUNT = src.INVAMOUNT,
			tgt.AMOUNTPAID = src.AMOUNTPAID,
			tgt.SLSNO = src.SLSNO,
			tgt.KODECABANG = src.KODECABANG,
			tgt.INV_TYPE = src.INV_TYPE,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK35][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.CustNo,
				r.InvNo,
				r.InvDate,
				r.DueDate,
				r.InvAmount,
				r.AmountPaid,
				r.SlsNo,
				r.Kodecabang,
				r.InvType,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk39(ctx context.Context, db *sql.DB, ch <-chan model.ImStkbal, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk39")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fstockbarang",
			"#tmp_fstockbarang",
			[]string{"KG", "PCODE", "STOCK", "KODECABANG", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fstockbarang (
				KG NVARCHAR(255),
				PCODE NVARCHAR(255),
				STOCK INT,
				KODECABANG NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.KG = src.KG AND tgt.PCODE = src.PCODE AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.KG = src.KG,
			tgt.PCODE = src.PCODE,
			tgt.STOCK = src.STOCK,
			tgt.KODECABANG = src.KODECABANG,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK39][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Kg,
				r.Pcode,
				r.Stock,
				r.Kodecabang,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk108(ctx context.Context, db *sql.DB, ch <-chan model.MBackOrder, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk108")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.forder_hd_status",
			"#tmp_forder_hd_status",
			[]string{"TGLORDER", "ORDERNO", "SLSNO", "CUSTNO", "KODECABANG", "ORDERNO_TOPUP", "PCODE", "STATUS", "STATUS_DETAIL", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_forder_hd_status (
				TGLORDER NVARCHAR(255),
				ORDERNO NVARCHAR(255),
				SLSNO NVARCHAR(255),
				CUSTNO NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				ORDERNO_TOPUP NVARCHAR(255),
				PCODE NVARCHAR(255),
				STATUS NVARCHAR(255),
				STATUS_DETAIL NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.TGLORDER = src.TGLORDER AND tgt.ORDERNO = src.ORDERNO AND tgt.SLSNO = src.SLSNO AND tgt.CUSTNO = src.CUSTNO AND tgt.KODECABANG = src.KODECABANG AND tgt.ORDERNO_TOPUP = src.ORDERNO_TOPUP AND tgt.PCODE = src.PCODE",
			`
			tgt.TGLORDER = src.TGLORDER,
			tgt.ORDERNO = src.ORDERNO,
			tgt.SLSNO = src.SLSNO,
			tgt.CUSTNO = src.CUSTNO,
			tgt.KODECABANG = src.KODECABANG,
			tgt.ORDERNO_TOPUP = src.ORDERNO_TOPUP,
			tgt.PCODE = src.PCODE,
			tgt.STATUS = src.STATUS,
			tgt.STATUS_DETAIL = src.STATUS_DETAIL,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK108][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.TglOrder,
				r.OrderNo,
				r.SlsNo,
				r.CustNo,
				r.Kodecabang,
				r.OrderNoTopUp,
				r.Pcode,
				r.Status,
				r.StatusDetail,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk103(ctx context.Context, db *sql.DB, ch <-chan model.Mbeat, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk103")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.gm_cust_wilayah",
			"#tmp_gm_cust_wilayah",
			[]string{"wc_district_id", "wc_wilayah_id", "wc_wilayah_desc", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_gm_cust_wilayah (
				wc_district_id NVARCHAR(255),
				wc_wilayah_id NVARCHAR(255),
				wc_wilayah_desc NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.wc_district_id = src.wc_district_id AND tgt.wc_wilayah_id = src.wc_wilayah_id",
			`
			tgt.wc_district_id = src.wc_district_id,
			tgt.wc_wilayah_id = src.wc_wilayah_id,
			tgt.wc_wilayah_desc = src.wc_wilayah_desc,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK103][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.WcDistrictId,
				r.WcWilayahId,
				r.WcWilayahDesc,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk44(ctx context.Context, db *sql.DB, ch <-chan model.McustCl, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk44")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fcredit_limit",
			"#tmp_fcredit_limit",
			[]string{"CUSTNO", "CUSTNAME", "CREDIT_LIMIT", "SISA_CREDIT_LIMIT", "KODECABANG", "UPDATEBY", "UPDATEDATE"},
			`
			CREATE TABLE #tmp_fcredit_limit (
				CUSTNO NVARCHAR(255),
				CUSTNAME NVARCHAR(255),
				CREDIT_LIMIT INT,
				SISA_CREDIT_LIMIT INT,
				KODECABANG NVARCHAR(255),
				UPDATEBY NVARCHAR(255),
				UPDATEDATE DATETIME
			)
			`,
			"tgt.CUSTNO = src.CUSTNO AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.CUSTNO = src.CUSTNO,
			tgt.CUSTNAME = src.CUSTNAME,
			tgt.CREDIT_LIMIT = src.CREDIT_LIMIT,
			tgt.SISA_CREDIT_LIMIT = src.SISA_CREDIT_LIMIT,
			tgt.KODECABANG = src.KODECABANG,
			tgt.UPDATEDATE = src.UPDATEDATE,
			tgt.UPDATEBY = src.UPDATEBY
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK44][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.CustNo,
				r.CustName,
				r.CreditLimit,
				r.SisaCreditLimit,
				r.Kodecabang,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk112(ctx context.Context, db *sql.DB, ch <-chan model.McustInvD, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk112")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fmst_custinv_d",
			"#tmp_fmst_custinv_d",
			[]string{"BID", "BNAME", "MUID", "MUNAME", "CUSTNO", "CUSTNAME", "INVNO", "INVDATE", "DUEDATE", "INV_AMOUNT", "INV_OUTSTANDING", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fmst_custinv_d (
				BID NVARCHAR(255),
				BNAME NVARCHAR(255),
				MUID NVARCHAR(255),
				MUNAME NVARCHAR(255),
				CUSTNO NVARCHAR(255),
				CUSTNAME NVARCHAR(255),
				INVNO NVARCHAR(255),
				INVDATE NVARCHAR(255),
				DUEDATE NVARCHAR(255),
				INV_AMOUNT FLOAT,
				INV_OUTSTANDING FLOAT,
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.BID = src.BID AND tgt.MUID = src.MUID AND tgt.CUSTNO = src.CUSTNO AND tgt.INVNO = src.INVNO",
			`
			tgt.BID = src.BID,
			tgt.BNAME = src.BNAME,
			tgt.MUID = src.MUID,
			tgt.MUNAME = src.MUNAME,
			tgt.CUSTNO = src.CUSTNO,
			tgt.CUSTNAME = src.CUSTNAME,
			tgt.INVNO = src.INVNO,
			tgt.INVDATE = src.INVDATE,
			tgt.DUEDATE = src.DUEDATE,
			tgt.INV_AMOUNT = src.INV_AMOUNT,
			tgt.INV_OUTSTANDING = src.INV_OUTSTANDING,
			tgt.STATUS_DETAIL = src.STATUS_DETAIL,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK112][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Bid,
				r.Bname,
				r.MuId,
				r.MuName,
				r.CustNo,
				r.CustName,
				r.InvNo,
				r.InvDate,
				r.DueDate,
				r.InvAmount,
				r.InvOutStanding,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk111(ctx context.Context, db *sql.DB, ch <-chan model.McustInvH, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk111")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fmst_custinv_h",
			"#tmp_fmst_custinv_h",
			[]string{"BID", "BNAME", "MUID", "MUNAME", "CUSTNO", "CUSTNAME", "INV_TOTAL", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fmst_custinv_h (
				BID NVARCHAR(225),
				BNAME NVARCHAR(225),
				MUID NVARCHAR(225),
				MUNAME NVARCHAR(225),
				CUSTNO NVARCHAR(225),
				CUSTNAME NVARCHAR(225),
				INV_TOTAL NVARCHAR(225),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.BID = src.BID AND tgt.MUID = src.MUID AND tgt.CUSTNO = src.CUSTNO",
			`
			tgt.BID = src.BID,
			tgt.BNAME = src.BNAME,
			tgt.MUID = src.MUID,
			tgt.MUNAME = src.MUNAME,
			tgt.CUSTNO = src.CUSTNO,
			tgt.CUSTNAME = src.CUSTNAME,
			tgt.INV_TOTAL = src.INV_TOTAL,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[BULK112][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Bid,
				r.Bname,
				r.MuId,
				r.MuName,
				r.CustNo,
				r.CustName,
				r.InvTotal,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk03(ctx context.Context, db *sql.DB, ch <-chan model.McustType, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk03")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.ftypeout",
			"#tmp_ftypeout",
			[]string{"TYPE", "TYPENAME", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_ftypeout (
				TYPE NVARCHAR(255),
				TYPENAME NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.TYPE = src.TYPE",
			`
			tgt.TYPE = src.TYPE,
			tgt.TYPENAME = src.TYPENAME,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk03][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Type,
				r.TypeName,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk102(ctx context.Context, db *sql.DB, ch <-chan model.MDistrict, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk102")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fdistrik",
			"#tmp_fdistrik",
			[]string{"KODECABANG", "DISTRIK", "DISTRIKNAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fdistrik (
				KODECABANG NVARCHAR(255),
				DISTRIK NVARCHAR(255),
				DISTRIKNAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.DISTRIK = src.DISTRIK AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.KODECABANG = src.KODECABANG,
			tgt.DISTRIK = src.DISTRIK,
			tgt.DISTRIKNAME = src.DISTRIKNAME,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk102][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.KodeCabang,
				r.Distrik,
				r.DistrikName,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk46(ctx context.Context, db *sql.DB, ch <-chan model.Mkat, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk46")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fkategori",
			"#tmp_fkategori",
			[]string{"KODE", "KET", "KODEDISTRIBUTOR", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fkategori (
				KODE NVARCHAR(255),
				KET NVARCHAR(255),
				KODEDISTRIBUTOR NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.KODE = src.KODE AND tgt.KODEDISTRIBUTOR = src.KODEDISTRIBUTOR",
			`
			tgt.KODE = src.KODE,
			tgt.KET = src.KET,
			tgt.KODEDISTRIBUTOR = src.KODEDISTRIBUTOR,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk46][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Kode,
				r.Ket,
				r.KodeDistributor,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk113(ctx context.Context, db *sql.DB, ch <-chan model.MkplPrice, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk113")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.mkplprice_dummy",
		[]string{
			"UNIQ_ID",
			"LINE_NO",
			"CUST_CODE",
			"BRANCH_ID",
			"PCODE",
			"PRICE_VALUE",
			"PRICE_UOM",
			"CBY",
			"CDATE",
			"MBY",
			"MDATE",
			"CORE_FILENAME",
			"CORE_PROCESSDATE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.UniqID,
				r.LineNo,
				r.CustCode,
				r.Pcode,
				r.PriceValue,
				r.PriceUom,
				r.BranchID,
				r.Cby,
				r.Cdate,
				r.Mby,
				r.Mdate,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk105(ctx context.Context, db *sql.DB, ch <-chan model.Mmarket, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk105")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.gm_cust_market",
			"#tmp_gm_cust_market",
			[]string{"psr_pasar_id", "psr_long_desc", "psr_short_desc", "kodecabang", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_gm_cust_market (
				psr_pasar_id NVARCHAR(255),
				psr_long_desc NVARCHAR(255),
				psr_short_desc NVARCHAR(255),
				kodecabang NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.psr_pasar_id = src.psr_pasar_id AND tgt.kodecabang = src.kodecabang",
			`
			tgt.psr_pasar_id = src.psr_pasar_id,
			tgt.psr_long_desc = src.psr_long_desc,
			tgt.psr_short_desc = src.psr_short_desc,
			tgt.kodecabang = src.kodecabang,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk105][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.PsrPasarId,
				r.PsrLongDesc,
				r.PsrShortDesc,
				r.Kodecabang,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk110(ctx context.Context, db *sql.DB, ch <-chan model.MPayerTo, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk110")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.FMST_PAYTO",
			"#tmp_fmst_payto",
			[]string{"CUSTNO", "CUSTNO_BIL", "DESC_CUSTNO_BIL", "KODECABANG", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fmst_payto (
				CUSTNO NVARCHAR(255),
				CUSTNO_BIL NVARCHAR(255),
				DESC_CUSTNO_BIL NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.KODECABANG = src.KODECABANG AND tgt.CUSTNO = src.CUSTNO",
			`
			tgt.CUSTNO = src.CUSTNO,
			tgt.CUSTNO_BIL = src.CUSTNO_BIL,
			tgt.DESC_CUSTNO_BIL = src.DESC_CUSTNO_BIL,
			tgt.KODECABANG = src.KODECABANG,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk105][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.CustNo,
				r.CustNoBil,
				r.DescCustNoBil,
				r.Kodecabang,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk101(ctx context.Context, db *sql.DB, ch <-chan model.MProvince, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk101")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fprovinsi",
			"#tmp_fprovinsi",
			[]string{"PROVINSI_ID", "PROVINSI_NAME", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fprovinsi (
				PROVINSI_ID NVARCHAR(255),
				PROVINSI_NAME NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.PROVINSI_ID = src.PROVINSI_ID",
			`
			tgt.PROVINSI_ID = src.PROVINSI_ID,
			tgt.PROVINSI_NAME = src.PROVINSI_NAME,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk101][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProvinsiId,
				r.ProvinsiName,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk19(ctx context.Context, db *sql.DB, ch <-chan model.MRute, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk19")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.frute",
			"#tmp_frute",
			[]string{"REGION", "CABANG", "KODECABANG", "SLSNO", "NORUTE", "CUSTNO", "H1", "H2", "H3", "H4", "H5", "H6", "H7", "M1", "M2", "M3", "M4", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_frute (
				REGION NVARCHAR(255),
				CABANG NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				SLSNO NVARCHAR(255),
				NORUTE NVARCHAR(255),
				CUSTNO NVARCHAR(255),
				H1 NVARCHAR(255),
				H2 NVARCHAR(255),
				H3 NVARCHAR(255),
				H4 NVARCHAR(255),
				H5 NVARCHAR(255),
				H6 NVARCHAR(255),
				H7 NVARCHAR(255),
				M1 NVARCHAR(255),
				M2 NVARCHAR(255),
				M3 NVARCHAR(255),
				M4 NVARCHAR(255),
				PROVINSI_NAME NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.REGION = src.REGION AND tgt.CABANG = src.CABANG AND tgt.KODECABANG = src.KODECABANG AND tgt.SLSNO = src.SLSNO AND tgt.NORUTE = src.NORUTE AND tgt.CUSTNO = src.CUSTNO",
			`
			tgt.REGION = src.REGION,
			tgt.CABANG = src.CABANG,
			tgt.KODECABANG = src.KODECABANG,
			tgt.SLSNO = src.SLSNO,
			tgt.NORUTE = src.NORUTE,
			tgt.CUSTNO = src.CUSTNO,
			tgt.H1 = src.H1,
			tgt.H2 = src.H2,
			tgt.H3 = src.H3,
			tgt.H4 = src.H4,
			tgt.H5 = src.H5,
			tgt.H6 = src.H6,
			tgt.H7 = src.H7,
			tgt.M1 = src.M1,
			tgt.M2 = src.M2,
			tgt.M3 = src.M3,
			tgt.M4 = src.M4,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk101][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Region,
				r.Cabang,
				r.Kodecabang,
				r.SlsNo,
				r.NoRute,
				r.CustNo,
				r.HSatu,
				r.HDua,
				r.HTiga,
				r.HEmpat,
				r.HLima,
				r.HEnam,
				r.HTujuh,
				r.MSatu,
				r.MDua,
				r.MTiga,
				r.MEmpat,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk23(ctx context.Context, db *sql.DB, ch <-chan model.MSBrand, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk23")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fbrand",
			"#tmp_fbrand",
			[]string{"BRAND", "BRANDNAME", "KODECABANG", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fbrand (
				BRAND NVARCHAR(255),
				BRANDNAME NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.BRAND = src.BRAND AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.BRAND = src.BRAND,
			tgt.BRANDNAME = src.BRANDNAME,
			tgt.KODECABANG = src.KODECABANG,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk23][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Brand,
				r.BrandName,
				r.Kodecabang,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk109(ctx context.Context, db *sql.DB, ch <-chan model.MShipTo, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk109")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fshippto",
			"#tmp_fshippto",
			[]string{"CUSTNO", "CUSTNO_SHIP", "DESC_CUSTNO_SHIP", "KODECABANG", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fshippto (
				CUSTNO NVARCHAR(255),
				CUSTNO_SHIP NVARCHAR(255),
				DESC_CUSTNO_SHIP NVARCHAR(255),
				KODECABANG NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.CUSTNO = src.CUSTNO AND tgt.KODECABANG = src.KODECABANG",
			`
			tgt.CUSTNO = src.CUSTNO,
			tgt.CUSTNO_SHIP = src.CUSTNO_SHIP,
			tgt.DESC_CUSTNO_SHIP = src.DESC_CUSTNO_SHIP,
			tgt.KODECABANG = src.KODECABANG,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk109][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.CustNo,
				r.CustNoShip,
				r.DescCustNoShip,
				r.Kodecabang,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk22(ctx context.Context, db *sql.DB, ch <-chan model.MSline, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk22")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fprlin",
			"#tmp_fprlin",
			[]string{"PRLIN", "PRLINAME", "KOMPFLAG", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fprlin (
				PRLIN NVARCHAR(255),
				PRLINAME NVARCHAR(255),
				KOMPFLAG NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.PRLIN = src.PRLIN",
			`
			tgt.PRLIN = src.PRLIN,
			tgt.PRLINAME = src.PRLINAME,
			tgt.KOMPFLAG = src.KOMPFLAG,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk22][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Prlin,
				r.PrliName,
				r.KompFlag,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk104(ctx context.Context, db *sql.DB, ch <-chan model.MSubBeat, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk104")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.gm_cust_rayon",
			"#tmp_gm_cust_rayon",
			[]string{"rc_district_id", "rc_wilayah_id", "rc_rayon_id", "rc_rayon_desc", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_gm_cust_rayon (
				rc_district_id NVARCHAR(255),
				rc_wilayah_id NVARCHAR(255),
				rc_rayon_id NVARCHAR(255),
				rc_rayon_desc NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.rc_district_id = src.rc_district_id and tgt.rc_wilayah_id = src.rc_wilayah_id and tgt.rc_rayon_id = src.rc_rayon_id",
			`
			tgt.rc_district_id = src.rc_district_id,
			tgt.rc_wilayah_id = src.rc_wilayah_id,
			tgt.rc_rayon_id = src.rc_rayon_id,
			tgt.rc_rayon_desc = src.rc_rayon_desc,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk104][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.RcDistrictId,
				r.RcWilayahId,
				r.RcRayonId,
				r.RcRayonDesc,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk47(ctx context.Context, db *sql.DB, ch <-chan model.MSubBrand, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk47")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.fsubbrand",
			"#tmp_fsubbrand",
			[]string{"KODE", "BRAND", "KET", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_fsubbrand (
				KODE NVARCHAR(255),
				BRAND NVARCHAR(255),
				KET NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.KODE = src.KODE AND tgt.BRAND = src.BRAND",
			`
			tgt.KODE = src.KODE,
			tgt.BRAND = src.BRAND,
			tgt.KET = src.KET,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk104][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Kode,
				r.Brand,
				r.Ket,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk07(ctx context.Context, db *sql.DB, ch <-chan model.MTop, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk07")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTable(
			ctx,
			db,
			"dbo.ftop",
			"#tmp_ftop",
			[]string{"TOP", "TOP_DESC", "TOP_DAYS", "CORE_FILENAME", "CORE_PROCESSDATE"},
			`
			CREATE TABLE #tmp_ftop (
				TOP NVARCHAR(255),
				TOP_DESC NVARCHAR(255),
				TOP_DAYS NVARCHAR(255),
				CORE_FILENAME NVARCHAR(255),
				CORE_PROCESSDATE DATETIME
			)
			`,
			"tgt.TOP = src.TOP",
			`
			tgt.TOP = src.TOP,
			tgt.TOP_DESC = src.TOP_DESC,
			tgt.TOP_DAYS = src.TOP_DAYS,
			tgt.CORE_FILENAME = src.CORE_FILENAME,
			tgt.CORE_PROCESSDATE = src.CORE_PROCESSDATE
			`,
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk104][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.Top,
				r.TopDesc,
				r.TopDays,
				r.CoreFilename,
				r.CoreProcessdate,
			}
		}
	}
	close(rows)
}

func Bulk120(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZdhdr, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk120")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.DP_ZDHDR",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"CONDITIONTYPE",
			"KEYCOMBINATION",
			"KEYCOMB",
			"SALESORGANIZATION",
			"DISTRIBUTIONCHANNEL",
			"SALESOFFICE",
			"DIVISION",
			"PAYMENTTERM",
			"CUSTOMER",
			"MATERIAL",
			"ATTRIBUT2",
			"VALIDUNTIL",
			"VALIDFROM",
			"CONDITIONRECORDNO",
			"SCALE",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.ConditionType,
				r.Keycombination,
				r.Keycomb,
				r.SalesOrganization,
				r.DistributionChannel,
				r.SalesOffice,
				r.Division,
				r.PaymentTerm,
				r.Customer,
				r.Material,
				r.Attribut2,
				r.ValidUntil,
				r.ValidFrom,
				r.ConditionRecordno,
				r.Scale,
				r.FileName,
				r.LineNumber,
				r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk121(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZditm, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk121")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.DP_ZDITM",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"CONDITIONTYPE",
			"KEYCOMBINATION",
			"KEYCOMB",
			"SALESORGANIZATION",
			"DISTRIBUTIONCHANNEL",
			"SALESOFFICE",
			"DIVISION",
			"SOLDTOPARTY",
			"PRICINGREFMATL",
			"PAYMENTTERMS",
			"INDUSTRYCODE3",
			"INDUSTRYCODE4",
			"INDUSTRYCODE5",
			"ATTRIBUTE1",
			"ATTRIBUTE2",
			"MATERIAL",
			"SALESUNIT",
			"VALIDFROM",
			"VALIDUNTIL",
			"CONDITIONRECORDNO",
			"SCALE",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.ConditionType,
				r.KeyCombination,
				r.KeyComb,
				r.SalesOrganization,
				r.DistributionChannel,
				r.SalesOffice,
				r.Division,
				r.SoldToParty,
				r.PricingRefMatl,
				r.PaymentTerms,
				r.IndustryCode3,
				r.IndustryCode4,
				r.IndustryCode5,
				r.Attribute1,
				r.Attribute2,
				r.Material,
				r.SalesUnit,
				r.ValidFrom,
				r.ValidUntil,
				r.ConditionRecordNo,
				r.Scale,
				r.FileName,
				r.LineNumber,
				r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk122(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZddet, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk122")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.DP_ZDDET",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"CONDITIONRECORDNO",
			"AMOUNT",
			"UNIT",
			"PER",
			"UOM",
			"SCALE",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.ConditionRecordNo,
				r.Amount,
				r.Unit,
				r.Per,
				r.Uom,
				r.Scale,
				r.Filename,
				r.Linenumber,
				r.Cdate,
			}
		}
	}
	close(rows)
}

func Bulk123(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZpmix, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk123")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTableRowNumber(
			ctx,
			db,
			"dbo.DP_ZPMIX",
			"#tmp_DP_ZPMIX",
			[]string{"PROCESS_ID", "BLOCKID", "BLOCKNAME", "CTYP", "KEYCOMBINATION", "SORG", "DCHL", "SOFF", "DV", "CUSTOMER", "INDCODE2", "INDCODE3", "INDCODE4", "INDCODE5", "PL", "PAYT", "MATERIAL", "VALIDFROM", "VALIDUNTIL", "PROMOID", "LINEITEM", "FILENAME", "LINENUMBER", "CDATE", "MUSTBUY", "EXCLUDE", "SPLIT", "AMOUNTX", "RANGEX", "WITHMATERIAL", "KELIPATAN", "V_KELIPATAN", "ATTR_PRD_LV2", "ATTR_PRD_LV3", "FL_CUST_EXC", "CUST_EXC", "FL_HD", "PERBANDINGAN", "V_PERBANDINGAN1", "V_PERBANDINGAN2"},
			`
			CREATE TABLE #tmp_DP_ZPMIX (
				PROCESS_ID NVARCHAR(50),
				BLOCKID NVARCHAR(3),
				BLOCKNAME NVARCHAR(50),
				CTYP NVARCHAR(20),
				KEYCOMBINATION NVARCHAR(20),
				SORG NVARCHAR(20),
				DCHL NVARCHAR(20),
				SOFF NVARCHAR(20),
				DV NVARCHAR(20),
				CUSTOMER NVARCHAR(20),
				INDCODE2 NVARCHAR(20),
				INDCODE3 NVARCHAR(20),
				INDCODE4 NVARCHAR(20),
				INDCODE5 NVARCHAR(20),
				PL NVARCHAR(20),
				PAYT NVARCHAR(20),
				MATERIAL NVARCHAR(20),
				VALIDFROM DATE,
				VALIDUNTIL DATE,
				PROMOID NVARCHAR(20),
				LINEITEM INT,
				FILENAME NVARCHAR(200),
				LINENUMBER BIGINT,
				CDATE DATETIME,
				MUSTBUY NVARCHAR(5),
				EXCLUDE NVARCHAR(5),
				SPLIT NVARCHAR(5),
				AMOUNTX NVARCHAR(1),
				RANGEX NVARCHAR(5),
				WITHMATERIAL NVARCHAR(5),
				KELIPATAN NVARCHAR(5),
				V_KELIPATAN INT,
				ATTR_PRD_LV2 NVARCHAR(20),
				ATTR_PRD_LV3 NVARCHAR(20),
				FL_CUST_EXC NVARCHAR(20),
				CUST_EXC NVARCHAR(20),
				FL_HD NVARCHAR(100),
				PERBANDINGAN NVARCHAR(20),
				V_PERBANDINGAN1 INT,
				V_PERBANDINGAN2 INT
			)
			`,
			"tgt.BLOCKID = src.BLOCKID AND tgt.PROMOID = src.PROMOID AND tgt.LINEITEM = src.LINEITEM AND tgt.CTYP = src.CTYP AND tgt.KEYCOMBINATION = src.KEYCOMBINATION AND tgt.SORG = src.SORG AND tgt.DCHL = src.DCHL AND tgt.SOFF = src.SOFF AND tgt.DV = src.DV AND tgt.CUSTOMER = src.CUSTOMER AND tgt.PL = src.PL AND tgt.PAYT = src.PAYT AND tgt.MATERIAL = src.MATERIAL",
			`
			tgt.PROCESS_ID = src.PROCESS_ID,
			tgt.BLOCKID = src.BLOCKID,
			tgt.BLOCKNAME = src.BLOCKNAME,
			tgt.CTYP = src.CTYP,
			tgt.KEYCOMBINATION = src.KEYCOMBINATION,
			tgt.SORG = src.SORG,
			tgt.DCHL = src.DCHL,
			tgt.SOFF = src.SOFF,
			tgt.DV = src.DV,
			tgt.CUSTOMER = src.CUSTOMER,
			tgt.INDCODE2 = src.INDCODE2,
			tgt.INDCODE3 = src.INDCODE3,
			tgt.INDCODE4 = src.INDCODE4,
			tgt.INDCODE5 = src.INDCODE5,
			tgt.PL = src.PL,
			tgt.PAYT = src.PAYT,
			tgt.MATERIAL = src.MATERIAL,
			tgt.VALIDFROM = src.VALIDFROM,
			tgt.VALIDUNTIL = src.VALIDUNTIL,
			tgt.PROMOID = src.PROMOID,
			tgt.LINEITEM = src.LINEITEM,
			tgt.FILENAME = src.FILENAME,
			tgt.LINENUMBER = src.LINENUMBER,
			tgt.CDATE = src.CDATE,
			tgt.MUSTBUY = src.MUSTBUY,
			tgt.EXCLUDE = src.EXCLUDE,
			tgt.SPLIT = src.SPLIT,
			tgt.AMOUNTX = src.AMOUNTX,
			tgt.RANGEX = src.RANGEX,
			tgt.WITHMATERIAL = src.WITHMATERIAL,
			tgt.KELIPATAN = src.KELIPATAN,
			tgt.V_KELIPATAN = src.V_KELIPATAN,
			tgt.ATTR_PRD_LV2 = src.ATTR_PRD_LV2,
			tgt.ATTR_PRD_LV3 = src.ATTR_PRD_LV3,
			tgt.FL_CUST_EXC = src.FL_CUST_EXC,
			tgt.CUST_EXC = src.CUST_EXC,
			tgt.FL_HD = src.FL_HD,
			tgt.PERBANDINGAN = src.PERBANDINGAN,
			tgt.V_PERBANDINGAN1 = src.V_PERBANDINGAN1,
			tgt.V_PERBANDINGAN2 = src.V_PERBANDINGAN2
			`,
			"src.BLOCKID, src.PROMOID, src.LINEITEM, src.CTYP, src.KEYCOMBINATION, src.SORG, src.DCHL, src.SOFF, src.DV, src.CUSTOMER, src.PL, src.PAYT, src.MATERIAL",
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk123][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.Blockname,
				r.Ctyp,
				r.KeyCombination,
				r.Sorg,
				r.Dchl,
				r.Soff,
				r.Dv,
				r.Customer,
				r.Indcode2,
				r.Indcode3,
				r.Indcode4,
				r.Indcode5,
				r.Pl,
				r.Payt,
				r.Material,
				r.ValidFrom,
				r.ValidUntil,
				r.PromoId,
				r.LineItem,
				r.FileName,
				r.LineNumber,
				r.Cdate,
				r.MustBuy,
				r.Exclude,
				r.Split,
				r.Amountx,
				r.Rangex,
				r.WithMaterial,
				r.Kelipatan,
				r.VKelipatan,
				r.AttrPrdLv2,
				r.AttrPrdLv3,
				r.FlCustExc,
				r.CustExc,
				r.FlHd,
				r.Perbandingan,
				r.VPerbandingan1,
				r.VPerbandingan2,
			}
		}
	}
	close(rows)
}

func Bulk123Promo(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZpmix, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk123Promo")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTableRowNumber(
			ctx,
			db,
			"dbo.DP_FG_CHECK",
			"#tmp_DP_FG_CHECK",
			[]string{"PROCESS_ID", "BLOCKID", "BLOCKNAME", "PROMOID", "DDATE", "CDATE"},
			`
			CREATE TABLE #tmp_DP_FG_CHECK (
				PROCESS_ID NVARCHAR(255),
				BLOCKID NVARCHAR(255),
				BLOCKNAME NVARCHAR(255),
				PROMOID NVARCHAR(255),
				DDATE DATE,
				CDATE DATETIME
			)
			`,
			"tgt.BLOCKID = src.BLOCKID AND tgt.PROMOID = src.PROMOID AND tgt.DDATE = src.DDATE",
			`
			tgt.PROCESS_ID = src.PROCESS_ID,
			tgt.BLOCKID = src.BLOCKID,
			tgt.BLOCKNAME = src.BLOCKNAME,
			tgt.PROMOID = src.PROMOID,
			tgt.DDATE = src.DDATE,
			tgt.CDATE = src.CDATE
			`,
			"src.BLOCKID, src.PROMOID, src.DDATE",
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk123Promo][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.Blockname,
				r.PromoId,
				r.Cdate,
				r.Cdate,
			}
		}
	}
	close(rows)
}

func Bulk124(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZscreg, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk124")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTableRowNumber(
			ctx,
			db,
			"dbo.DP_ZSCREG",
			"#tmp_DP_ZSCREG",
			[]string{
				"PROCESS_ID",
				"BLOCKID",
				"BLOCKNAME",
				"CONDITIONRECORDNO",
				"NO",
				"LSNO",
				"DISCREGHDRQTY",
				"AMOUNT",
				"UNIT",
				"FILENAME",
				"LINENUMBER",
				"CDATE",
			},
			`
			CREATE TABLE #tmp_DP_ZSCREG (
				PROCESS_ID NVARCHAR(50),
				BLOCKID NVARCHAR(3),
				BLOCKNAME NVARCHAR(50),
				CONDITIONRECORDNO NVARCHAR(20),
				NO INT,
				LSNO INT,
				DISCREGHDRQTY DECIMAL(19,4),
				AMOUNT DECIMAL(19,4),
				UNIT NVARCHAR(25),
				FILENAME NVARCHAR(200),
				LINENUMBER BIGINT,
				CDATE DATETIME
			)
			`,
			"tgt.BLOCKID = src.BLOCKID AND tgt.CONDITIONRECORDNO = src.CONDITIONRECORDNO AND tgt.DISCREGHDRQTY = src.DISCREGHDRQTY",
			`
			tgt.PROCESS_ID = src.PROCESS_ID,
			tgt.BLOCKID = src.BLOCKID,
			tgt.BLOCKNAME = src.BLOCKNAME,
			tgt.CONDITIONRECORDNO = src.CONDITIONRECORDNO,
			tgt.NO = src.NO,
			tgt.LSNO = src.LSNO,
			tgt.DISCREGHDRQTY = src.DISCREGHDRQTY,
			tgt.AMOUNT = src.AMOUNT,
			tgt.UNIT = src.UNIT,
			tgt.FILENAME = src.FILENAME,
			tgt.LINENUMBER = src.LINENUMBER,
			tgt.CDATE = src.CDATE
			`,
			"src.BLOCKID, src.CONDITIONRECORDNO, src.DISCREGHDRQTY",
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk124][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.ConditionRecordNo,
				r.No,
				r.Lsno,
				r.DiscRegHdrQty,
				r.Amount,
				r.Unit,
				r.FileName,
				r.LineNumber,
				r.Cdate,
			}
		}
	}
	close(rows)
}

func Bulk125(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZscmix, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk125")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.DP_ZSCMIX",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"PROMOID",
			"LINEITEM",
			"SCALEQTY",
			"BUN",
			"AMOUNT",
			"UNIT",
			"PER",
			"UOM",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
			"SCALEQTYTO",
			"AMOUNTSCL",
			"AMOUNTSCLTO",
			"UNITSCL",
			"MATNRKENA",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.PromoId,
				r.LineItem,
				r.ScaleQty,
				r.Bun,
				r.Amount,
				r.Unit,
				r.Per,
				r.Uom,
				r.FileName,
				r.LineNumber,
				r.Cdate,
				r.ScaleQtyTo,
				r.AmountScl,
				r.AmountSclTo,
				r.UnitScl,
				r.MatnrKena,
			}
		}
	}
	close(rows)
}

func Bulk126(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesDpZ00001, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk126")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.DP_Z00001",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"STEP",
			"COUNTER",
			"CONDITIONTYPE",
			"DESCRIPTION",
			"VALIDFROM",
			"VALIDTO",
			"CONDGRP",
			"DRULE",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
			"DISCTYPE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.Step,
				r.Counter,
				r.ConditionType,
				r.Description,
				r.ValidFrom,
				r.ValidTo,
				r.CondGrp,
				r.Drule,
				r.FileName,
				r.LineNumber,
				r.Cdate,
				r.DiscType,
			}
		}
	}
	close(rows)
}

func Bulk130(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesFgZdhdr, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk130")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTableRowNumber(
			ctx,
			db,
			"dbo.FG_ZDHDR",
			"#tmp_FG_ZDHDR",
			[]string{
				"PROCESS_ID",
				"BLOCKID",
				"BLOCKNAME",
				"CONDITIONTYPE",
				"KEYCOMBINATION",
				"KEYCOMB",
				"SALESORGANIZATION",
				"DISTRIBUTIONCHANNEL",
				"DIVISION",
				"SALESOFFICE",
				"PRICELISTTYPE",
				"ATTRIBUTE1",
				"INDUSTRYCODE3",
				"INDUSTRYCODE4",
				"INDUSTRYCODE5",
				"SOLDTOPARTY",
				"MATERIAL",
				"VALIDUNTIL",
				"VALIDFROM",
				"CONDITIONRECORDNO",
				"PROMOID",
				"PROMOITEM",
				"SCALE",
				"FILENAME",
				"LINENUMBER",
				"CDATE",
				"MUSTBUY",
				"KELIPATAN",
				"F_KELIPATAN",
				"WITHQTY",
				"QTY",
				"UOM",
				"ZTERM",
				"KATR2",
				"KATR3",
				"PERBANDINGAN",
				"F_PERBANDINGAN1",
				"F_PERBANDINGAN2",
				"AMOUNTX",
			},
			`
			CREATE TABLE #tmp_FG_ZDHDR (
				PROCESS_ID NVARCHAR(50),
				BLOCKID NVARCHAR(3),
				BLOCKNAME NVARCHAR(50),
				CONDITIONTYPE NVARCHAR(20),
				KEYCOMBINATION NVARCHAR(20),
				KEYCOMB NVARCHAR(180),
				SALESORGANIZATION NVARCHAR(20),
				DISTRIBUTIONCHANNEL NVARCHAR(20),
				DIVISION NVARCHAR(20),
				SALESOFFICE NVARCHAR(20),
				PRICELISTTYPE NVARCHAR(20),
				ATTRIBUTE1 NVARCHAR(20),
				INDUSTRYCODE3 NVARCHAR(20),
				INDUSTRYCODE4 NVARCHAR(20),
				INDUSTRYCODE5 NVARCHAR(20),
				SOLDTOPARTY NVARCHAR(20),
				MATERIAL NVARCHAR(20),
				VALIDUNTIL DATE,
				VALIDFROM DATE,
				CONDITIONRECORDNO NVARCHAR(20),
				PROMOID NVARCHAR(20),
				PROMOITEM NVARCHAR(20),
				[SCALE] NVARCHAR(3),
				FILENAME NVARCHAR(100),
				LINENUMBER BIGINT,
				CDATE DATETIME,
				MUSTBUY NVARCHAR(5),
				KELIPATAN NVARCHAR(5),
				F_KELIPATAN INT,
				WITHQTY NVARCHAR(20),
				QTY INT,
				UOM FLOAT,
				ZTERM NVARCHAR(5),
				KATR2 NVARCHAR(20),
				KATR3 NVARCHAR(20),
				PERBANDINGAN NVARCHAR(20),
				F_PERBANDINGAN1 INT,
				F_PERBANDINGAN2 INT,
				AMOUNTX NVARCHAR(1)
			)
			`,
			"tgt.BLOCKID = src.BLOCKID AND tgt.PROMOID = src.PROMOID AND tgt.PROMOITEM = src.PROMOITEM AND tgt.CONDITIONRECORDNO = src.CONDITIONRECORDNO AND tgt.CONDITIONTYPE = src.CONDITIONTYPE AND tgt.KEYCOMBINATION = src.KEYCOMBINATION AND tgt.SALESORGANIZATION = src.SALESORGANIZATION AND tgt.DISTRIBUTIONCHANNEL = src.DISTRIBUTIONCHANNEL AND tgt.DIVISION = src.DIVISION AND tgt.SALESOFFICE = src.SALESOFFICE AND tgt.PRICELISTTYPE = src.PRICELISTTYPE AND tgt.ATTRIBUTE1 = src.ATTRIBUTE1 AND tgt.INDUSTRYCODE3 = src.INDUSTRYCODE3 AND tgt.INDUSTRYCODE4 = src.INDUSTRYCODE4 AND tgt.INDUSTRYCODE5 = src.INDUSTRYCODE5 AND tgt.SOLDTOPARTY = src.SOLDTOPARTY AND tgt.MATERIAL = src.MATERIAL AND tgt.ZTERM = src.ZTERM AND tgt.KATR2 = src.KATR2 AND tgt.KATR3 = src.KATR3",
			`
				tgt.PROCESS_ID = src.PROCESS_ID,
				tgt.BLOCKID = src.BLOCKID,
				tgt.BLOCKNAME = src.BLOCKNAME,
				tgt.CONDITIONTYPE = src.CONDITIONTYPE,
				tgt.KEYCOMBINATION = src.KEYCOMBINATION,
				tgt.KEYCOMB = src.KEYCOMB,
				tgt.SALESORGANIZATION = src.SALESORGANIZATION,
				tgt.DISTRIBUTIONCHANNEL = src.DISTRIBUTIONCHANNEL,
				tgt.DIVISION = src.DIVISION,
				tgt.SALESOFFICE = src.SALESOFFICE,
				tgt.PRICELISTTYPE = src.PRICELISTTYPE,
				tgt.ATTRIBUTE1 = src.ATTRIBUTE1,
				tgt.INDUSTRYCODE3 = src.INDUSTRYCODE3,
				tgt.INDUSTRYCODE4 = src.INDUSTRYCODE4,
				tgt.INDUSTRYCODE5 = src.INDUSTRYCODE5,
				tgt.SOLDTOPARTY = src.SOLDTOPARTY,
				tgt.MATERIAL = src.MATERIAL,
				tgt.VALIDUNTIL = src.VALIDUNTIL,
				tgt.VALIDFROM = src.VALIDFROM,
				tgt.CONDITIONRECORDNO = src.CONDITIONRECORDNO,
				tgt.PROMOID = src.PROMOID,
				tgt.PROMOITEM = src.PROMOITEM,
				tgt.SCALE = src.SCALE,
				tgt.FILENAME = src.FILENAME,
				tgt.LINENUMBER = src.LINENUMBER,
				tgt.CDATE = src.CDATE,
				tgt.MUSTBUY = src.MUSTBUY,
				tgt.KELIPATAN = src.KELIPATAN,
				tgt.F_KELIPATAN = src.F_KELIPATAN,
				tgt.WITHQTY = src.WITHQTY,
				tgt.QTY = src.QTY,
				tgt.UOM = src.UOM,
				tgt.ZTERM = src.ZTERM,
				tgt.KATR2 = src.KATR2,
				tgt.KATR3 = src.KATR3,
				tgt.PERBANDINGAN = src.PERBANDINGAN,
				tgt.F_PERBANDINGAN1 = src.F_PERBANDINGAN1,
				tgt.F_PERBANDINGAN2 = src.F_PERBANDINGAN2,
				tgt.AMOUNTX = src.AMOUNTX
			`,
			"src.BLOCKID, src.PROMOID, src.PROMOITEM, src.CONDITIONRECORDNO, src.CONDITIONTYPE, src.KEYCOMBINATION, src.SALESORGANIZATION, src.DISTRIBUTIONCHANNEL, src.DIVISION, src.SALESOFFICE, src.PRICELISTTYPE, src.ATTRIBUTE1, src.INDUSTRYCODE3, src.INDUSTRYCODE4, src.INDUSTRYCODE5, src.SOLDTOPARTY, src.MATERIAL, src.ZTERM, src.KATR2, src.KATR3",
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk130][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.ConditionType,
				r.KeyCombination,
				r.KeyComb,
				r.SalesOrganization,
				r.DistributionChannel,
				r.Division,
				r.SalesOffice,
				r.PricelistType,
				r.Attribute1,
				r.IndustryCode3,
				r.IndustryCode4,
				r.IndustryCode5,
				r.SoldToParty,
				r.Material,
				r.ValidUntil,
				r.ValidFrom,
				r.ConditionRecordNo,
				r.PromoId,
				r.PromoItem,
				r.Scale,
				r.FileName,
				r.LineNumber,
				r.CDate,
				r.MustBuy,
				r.Kelipatan,
				r.FKelipatan,
				r.WithQty,
				r.Uom,
				r.Qty,
				r.Zterm,
				r.Katr2,
				r.Katr3,
				r.Perbandingan,
				r.FPerbandingan1,
				r.FPerbandingan2,
				r.Amountx,
			}
		}
	}
	close(rows)
}

func Bulk130Promo(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesFgZdhdr, done chan<- struct{}) {
	l, err := logger.NewDailyWorkerLogger("bulk130Promo")
	if err != nil {
		panic(err)
	}

	rows := make(chan func() []any, 1000)

	go func() {
		err := bulkUpsertViaTempTableRowNumber(
			ctx,
			db,
			"dbo.DP_FG_CHECK",
			"#tmp_DP_FG_CHECK",
			[]string{"PROCESS_ID", "BLOCKID", "BLOCKNAME", "PROMOID", "DDATE", "CDATE"},
			`
			CREATE TABLE #tmp_DP_FG_CHECK (
				PROCESS_ID NVARCHAR(255),
				BLOCKID NVARCHAR(255),
				BLOCKNAME NVARCHAR(255),
				PROMOID NVARCHAR(255),
				DDATE DATE,
				CDATE DATETIME
			)
			`,
			"tgt.BLOCKID = src.BLOCKID AND tgt.PROMOID = src.PROMOID AND tgt.DDATE = src.DDATE",
			`
			tgt.PROCESS_ID = src.PROCESS_ID,
			tgt.BLOCKID = src.BLOCKID,
			tgt.BLOCKNAME = src.BLOCKNAME,
			tgt.PROMOID = src.PROMOID,
			tgt.DDATE = src.DDATE,
			tgt.CDATE = src.CDATE
			`,
			"src.BLOCKID, src.PROMOID, src.DDATE",
			rows, done, l,
		)

		if err != nil {
			l.Printf("[Bulk123Promo][UPSERT] failed: %v", err)
		}
	}()

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.PromoId,
				r.CDate,
				r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk131(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesFgZfrdet, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk131")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.FG_ZFRDET",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"CONDITIONRECORDNO",
			"MINIMUMQTY",
			"FREEGOODSQTY",
			"UOMFREEGOODS",
			"FREEGOODSAGRREDQTY",
			"UOMFREEGOODSAGRRED",
			"ADDITIONALMATERIAL",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.ConditionRecordNo,
				r.MinimumQty,
				r.FreeGoodsQty,
				r.UomFreeGoods,
				r.FreeGoodsAgrredQty,
				r.UomFreeGoodsAgrred,
				r.AdditionalMaterial,
				r.FileName,
				r.LineNumber,
				r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk132(ctx context.Context, db *sql.DB, ch <-chan model.SpProsesFgZfrmix, done chan<- struct{}) {
	l, _ := logger.NewDailyWorkerLogger("bulk132")
	rows := make(chan func() []any, 2048)

	go bulkInsert(ctx, db, "dbo.FG_ZFRMIX",
		[]string{
			"PROCESS_ID",
			"BLOCKID",
			"BLOCKNAME",
			"PROMOID",
			"PROMOITEM",
			"SCALEQTY",
			"SCALEQTYUOM",
			"MATERIAL",
			"QTY",
			"QTYUOM",
			"FILENAME",
			"LINENUMBER",
			"CDATE",
			"AMOUNTSCLF",
			"CURRENCY",
		},
		rows, done, l,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.ProcessId,
				r.BlockId,
				r.BlockName,
				r.PromoId,
				r.PromoItem,
				r.ScaleQty,
				r.ScaleQtyUom,
				r.Material,
				r.Qty,
				r.QtyUom,
				r.FileName,
				r.LineNumber,
				r.CDate,
				r.AmountSclf,
				r.Currency,
			}
		}
	}
	close(rows)
}
