package logger

import (
	"log"
	"os"
	"path/filepath"
)

func NewWorkerLogger(workerName string) (*log.Logger, error) {
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(
		filepath.Join("logs", workerName+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return log.New(
		f,
		"",
		log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile,
	), nil
}
