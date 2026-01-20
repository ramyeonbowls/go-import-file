package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	JobName  string
	FilePath string

	FileDir        string
	FileSuccessDir string
	FileFailedDir  string
	LogsDir        string

	DBHost string
	DBPort string
	DBUser string
	DBPass string
	DBName string

	Worker             int
	BufferSize         int
	TimeoutSeconds     int
	IdleTimeoutSeconds int
	BatchSize          int

	UomBuy  string
	UomMain string

	FTP FTPConfig
}

type FTPConfig struct {
	Host                string
	Port                int
	Username            string
	Password            string
	RemoteDir           string
	FilePattern         string
	ArchiveDir          string
	DeleteAfterDownload bool
	MoveAfterDownload   bool
}

func Load() *Config {
	_ = godotenv.Load()

	workerCount, _ := strconv.Atoi(os.Getenv("WORKER_COUNT"))
	bufferSize, _ := strconv.Atoi(os.Getenv("BUFFER_SIZE"))
	timeoutSeconds, _ := strconv.Atoi(os.Getenv("TIMEOUT_SECONDS"))
	idleTimeoutSeconds, _ := strconv.Atoi(os.Getenv("IDLE_TIMEOUT_SECONDS"))
	batch, _ := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	port, _ := strconv.Atoi(os.Getenv("FTP_PORT"))
	deleteAfterDownload, _ := strconv.ParseBool(os.Getenv("FTP_DELETE"))
	moveAfterDownload, _ := strconv.ParseBool(os.Getenv("FTP_MOVE"))

	cfg := &Config{
		JobName:  os.Getenv("JOB_NAME"),
		FilePath: os.Getenv("FILE_PATH"),

		DBHost: os.Getenv("SQLSERVER_HOST"),
		DBPort: os.Getenv("SQLSERVER_PORT"),
		DBUser: os.Getenv("SQLSERVER_USER"),
		DBPass: os.Getenv("SQLSERVER_PASSWORD"),
		DBName: os.Getenv("SQLSERVER_DB"),

		FileDir:            os.Getenv("PROCESS_DIR"),
		FileSuccessDir:     os.Getenv("PROCESS_SUCCESS_DIR"),
		FileFailedDir:      os.Getenv("PROCESS_FAILED_DIR"),
		LogsDir:            os.Getenv("LOG_PATH"),
		UomBuy:             os.Getenv("UOM_BUY"),
		UomMain:            os.Getenv("UOM_MAIN"),
		Worker:             workerCount,
		BufferSize:         bufferSize,
		TimeoutSeconds:     timeoutSeconds,
		IdleTimeoutSeconds: idleTimeoutSeconds,
		BatchSize:          batch,

		FTP: FTPConfig{
			Host:                os.Getenv("FTP_HOST"),
			Port:                port,
			Username:            os.Getenv("FTP_USERNAME"),
			Password:            os.Getenv("FTP_PASSWORD"),
			RemoteDir:           os.Getenv("FTP_REMOTE_DIR"),
			FilePattern:         os.Getenv("FTP_FILE_PATTERN"),
			ArchiveDir:          os.Getenv("FTP_ARCHIVE_DIR"),
			DeleteAfterDownload: deleteAfterDownload,
			MoveAfterDownload:   moveAfterDownload,
		},
	}

	if cfg.FilePath == "" {
		log.Fatal("FILE_PATH wajib diisi")
	}

	return cfg
}
