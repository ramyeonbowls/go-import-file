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
		err := bulkUpsertViaTempTable(
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
				CLIMIT NVARCHAR(255),
				FLAGLIMIT NVARCHAR(255),
				GDISC NVARCHAR(255),
				GRUPOUT NVARCHAR(255),
				TYPEOUT NVARCHAR(255),
				GHARGA NVARCHAR(255),
				FLAGPAY NVARCHAR(255),
				FLAGOUT NVARCHAR(255),
				RPP NVARCHAR(255),
				LSALES NVARCHAR(255),
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
				r.Custno, r.Data01, r.CustName, r.CustAdd1,
				r.CustAdd2, r.City, r.Contact, r.Phone1,
				r.FaxNo, r.Cterm, r.Climit, r.FlagLimit,
				r.Gdisc, r.GrupOut, r.TypeOut, r.Gharga,
				r.FlagPay, r.FlagOut, r.Rpp, r.Lsales,
				r.Ldatetrs, r.Lokasi, r.Distrik, r.Beat,
				r.SubBeat, r.Klasif, r.Kindus, r.Kpasar,
				r.BranchID, r.La, r.Lg, r.CoreFilename, r.CoreProcessdate,
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
