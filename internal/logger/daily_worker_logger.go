package logger

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DailyWorkerLogger struct {
	worker string
	mu     sync.Mutex
	date   string
	file   *os.File
	logger *log.Logger
}

func NewDailyWorkerLogger(worker string) (*DailyWorkerLogger, error) {
	l := &DailyWorkerLogger{
		worker: worker,
	}
	if err := l.rotateIfNeeded(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DailyWorkerLogger) rotateIfNeeded() error {
	today := time.Now().Format("2006-01-02")

	if l.file != nil && l.date == today {
		return nil
	}

	if err := os.MkdirAll("logs", 0755); err != nil {
		return err
	}

	if l.file != nil {
		_ = l.file.Close()
	}

	path := filepath.Join(
		"logs",
		l.worker+"-"+today+".log",
	)

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	l.file = f
	l.date = today
	l.logger = log.New(
		f,
		"",
		log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile,
	)

	return nil
}

func (l *DailyWorkerLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_ = l.rotateIfNeeded()
	l.logger.Printf(format, v...)
}
