package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block132Handler struct {
	Out chan<- model.SpProsesFgZfrmix
}

func (h *Block132Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	ScaleQtyVal, _ := utils.ParseNumber(safe(fields, 4))
	QtyVal, _ := utils.ParseAccountingFloat(safe(fields, 7))
	AmountSclfVal, _ := utils.ParseAccountingFloat(safe(fields, 9))
	now := time.Now()

	h.Out <- model.SpProsesFgZfrmix{
		ProcessId:   processID,
		BlockId:     safe(fields, 0),
		BlockName:   safe(fields, 1),
		PromoId:     safe(fields, 2),
		PromoItem:   safe(fields, 3),
		ScaleQty:    ScaleQtyVal,
		ScaleQtyUom: safe(fields, 5),
		Material:    safe(fields, 6),
		Qty:         QtyVal,
		QtyUom:      safe(fields, 8),
		FileName:    job.FileName,
		LineNumber:  lineNo,
		CDate:       now,
		AmountSclf:  AmountSclfVal,
		Currency:    safe(fields, 10),
	}

	return nil
}
