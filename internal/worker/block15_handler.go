package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block15Handler struct {
	Out chan<- model.MpriceGrp
}

func (h *Block15Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MpriceGrp{
		PriceCode:       safe(fields, 2),
		PriceDesc:       safe(fields, 3),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
