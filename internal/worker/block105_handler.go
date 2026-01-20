package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block105Handler struct {
	Out chan<- model.Mmarket
}

func (h *Block105Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.Mmarket{
		PsrPasarId:      safe(fields, 2),
		PsrLongDesc:     safe(fields, 3),
		PsrShortDesc:    safe(fields, 4),
		Kodecabang:      safe(fields, 5),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
