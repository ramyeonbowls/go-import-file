package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block03Handler struct {
	Out chan<- model.McustType
}

func (h *Block03Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.McustType{
		Type:            safe(fields, 2),
		TypeName:        safe(fields, 3),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
