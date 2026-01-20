package db

import (
	"database/sql"
	"fmt"
	"go-import-file/internal/config"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

func NewSQLServer(host, port, user, pass, dbname string) (*sql.DB, error) {
	cfg := config.Load()

	dsn := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%s?database=%s",
		user, pass, host, port, dbname,
	)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.Worker + 2)
	db.SetMaxIdleConns(cfg.Worker)
	db.SetConnMaxLifetime(time.Duration(cfg.TimeoutSeconds) * time.Minute)

	return db, db.Ping()
}
