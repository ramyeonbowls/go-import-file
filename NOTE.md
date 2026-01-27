```sql
DROP TABLE dbo.import_finalize_log;

CREATE TABLE dbo.import_finalize_log (
	process_id varchar(36) NOT NULL,
	block_code varchar(50) NOT NULL,
	status varchar(20) NOT NULL,
	started_at datetime2 NOT NULL,
	finished_at datetime2 NULL,
	error_message nvarchar(4000) NULL,
	CONSTRAINT PK_import_finalize_log PRIMARY KEY (process_id,block_code)
);
```