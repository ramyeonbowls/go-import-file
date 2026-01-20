package logger

import (
	"log"
	"os"
	"sync"
)

var (
	once   sync.Once
	logger *log.Logger
)

// InitFileLogger harus dipanggil sekali di awal aplikasi
func InitFileLogger(path string) {
	once.Do(func() {
		f, err := os.OpenFile(
			path,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND,
			0644,
		)
		if err != nil {
			panic("failed to open log file: " + err.Error())
		}

		logger = log.New(
			f,
			"",
			log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile,
		)
	})
}

func L() *log.Logger {
	if logger == nil {
		panic("file logger not initialized")
	}
	return logger
}
