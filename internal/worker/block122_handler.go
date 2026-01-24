package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block122Handler struct {
	Out chan<- model.SpProsesDpZddet
}

func (h *Block122Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	AmountVal, _ := utils.ParseAccountingFloat(safe(fields, 3))
	Perval, _ := utils.ParseNumber(safe(fields, 5))
	now := time.Now()

	h.Out <- model.SpProsesDpZddet{
		ProcessId:         processID,
		BlockId:           safe(fields, 0),
		BlockName:         safe(fields, 1),
		ConditionRecordNo: safe(fields, 2),
		Amount:            AmountVal,
		Unit:              safe(fields, 4),
		Per:               Perval,
		Uom:               safe(fields, 6),
		Scale:             safe(fields, 7),
		Filename:          job.FileName,
		Linenumber:        lineNo,
		Cdate:             now,
	}

	return nil
}
