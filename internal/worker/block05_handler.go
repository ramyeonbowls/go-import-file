package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block05Handler struct {
	Out chan<- model.McustIndus
}

func (h *Block05Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.McustIndus{
		IndusId:         safe(fields, 2),
		IndusName:       safe(fields, 3),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
