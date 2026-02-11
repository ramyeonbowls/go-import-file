package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"strconv"
	"time"
)

type Block125Handler struct {
	Out chan<- model.SpProsesDpZscmix
}

func (h *Block125Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	LineItemVal, _ := strconv.Atoi(safe(fields, 3))
	ScaleQtyVal, _ := utils.ParseAccountingFloat(safe(fields, 4))
	AmountVal, _ := utils.ParseAccountingFloat(safe(fields, 6))
	PerVal, _ := utils.ParseAccountingFloat(safe(fields, 8))
	ScaleQtyToVal, _ := utils.ParseAccountingFloat(safe(fields, 10))
	AmountSclVal, _ := utils.ParseAccountingFloat(safe(fields, 11))
	AmountSclToVal, _ := utils.ParseAccountingFloat(safe(fields, 12))
	now := time.Now()

	h.Out <- model.SpProsesDpZscmix{
		ProcessId:   processID,
		BlockId:     safe(fields, 0),
		BlockName:   safe(fields, 1),
		PromoId:     safe(fields, 2),
		LineItem:    LineItemVal,
		ScaleQty:    ScaleQtyVal,
		Bun:         safe(fields, 3),
		Amount:      AmountVal,
		Unit:        safe(fields, 7),
		Per:         PerVal,
		Uom:         safe(fields, 9),
		FileName:    job.FileName,
		LineNumber:  lineNo,
		Cdate:       now,
		ScaleQtyTo:  ScaleQtyToVal,
		AmountScl:   AmountSclVal,
		AmountSclTo: AmountSclToVal,
		UnitScl:     safe(fields, 13),
		MatnrKena:   safe(fields, 14),
	}

	return nil
}
