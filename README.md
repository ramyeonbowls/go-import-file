# go-import-file

Simple Go importer for PDAMASTER-style transfer files. Parses files and writes data to SQL Server.

## Quick start

- Build: `go build cmd/main.go`
- Run: `go run cmd/main.go -block=EXAMPLE` or on Windows:

```powershell
./main -block=EXAMPLE
```

## Configuration

- Edit `internal/config/config.go` or provide environment variables as implemented there.
- DB helpers: `internal/db/sqlserver.go`.

## Project layout (short)

- `cmd/` – entrypoint
- `internal/` – `config`, `db`, `ftp` helpers
- `worker/` – parsers & block handlers
- `model/`, `orchestrator/`, `importer/`
- `logger/`, `metrics/`, `utils/`
- `transfer/` – processed files (`success/`, `failed/`)
- `logs/` – application logs

## Logs & transfers

- Successful files -> `transfer/success/`
- Failed files -> `transfer/failed/`
- Logs in `logs/`

## Contributing

- Open issues or PRs. Add a `LICENSE` file (e.g., MIT) if applicable.

If you want an example `internal/config/config.go` or a sample `config.example.json`, I can add one.
