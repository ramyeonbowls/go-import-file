package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block02Handler struct {
	Out chan<- model.McustGrp
}

func (h *Block02Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.McustGrp{
		GroupOut:        safe(fields, 2),
		GroupName:       safe(fields, 3),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
