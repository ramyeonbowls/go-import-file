package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block47Handler struct {
	Out chan<- model.MSubBrand
}

func (h *Block47Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MSubBrand{
		Kode:            safe(fields, 2),
		Brand:           safe(fields, 3),
		Ket:             safe(fields, 4),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
