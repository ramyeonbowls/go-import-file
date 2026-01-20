package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block113Handler struct {
	Out chan<- model.MkplPrice
}

func (h *Block113Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	PriceValueVal, _ := utils.ParseNumber(safe(fields, 4))
	now := time.Now()
	by := "system"

	h.Out <- model.MkplPrice{
		UniqID:          processID,
		LineNo:          lineNo,
		CustCode:        safe(fields, 2),
		Pcode:           safe(fields, 3),
		PriceValue:      PriceValueVal,
		PriceUom:        safe(fields, 5),
		BranchID:        safe(fields, 6),
		Cby:             by,
		Cdate:           now,
		Mby:             by,
		Mdate:           now,
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
