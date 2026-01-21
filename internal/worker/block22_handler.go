package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block22Handler struct {
	Out chan<- model.MSline
}

func (h *Block22Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MSline{
		Prlin:           safe(fields, 2),
		PrliName:        safe(fields, 3),
		KompFlag:        safe(fields, 4),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
