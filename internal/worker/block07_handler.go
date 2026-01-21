package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block07Handler struct {
	Out chan<- model.MTop
}

func (h *Block07Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MTop{
		Top:             safe(fields, 2),
		TopDesc:         safe(fields, 3),
		TopDays:         safe(fields, 4),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
