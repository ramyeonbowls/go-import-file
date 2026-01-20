package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block16Handler struct {
	Out chan<- model.Mprice
}

func (h *Block16Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	now := time.Now()
	by := "system"

	h.Out <- model.Mprice{
		UniqID:          processID,
		LineNo:          lineNo,
		PriceCode:       safe(fields, 2),
		BranchID:        safe(fields, 4),
		Pcode:           safe(fields, 3),
		PriceValue:      safe(fields, 9),
		PriceUom:        safe(fields, 10),
		Cby:             by,
		Cdate:           now,
		Mby:             by,
		Mdate:           now,
		CoreFilename:    job.FileName,
		CoreProcessdate: now,
	}

	return nil
}
