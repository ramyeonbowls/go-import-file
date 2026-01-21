package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block23Handler struct {
	Out chan<- model.MSBrand
}

func (h *Block23Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MSBrand{
		Brand:           safe(fields, 2),
		BrandName:       safe(fields, 3),
		Kodecabang:      safe(fields, 4),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
