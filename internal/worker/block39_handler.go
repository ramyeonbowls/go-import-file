package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block39Handler struct {
	Out chan<- model.ImStkbal
}

func (h *Block39Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	StockVal, _ := utils.ParseNumber(safe(fields, 5))

	h.Out <- model.ImStkbal{
		Kg:              safe(fields, 2),
		Pcode:           safe(fields, 3),
		Stock:           StockVal,
		Kodecabang:      safe(fields, 6),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
