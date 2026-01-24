package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"strconv"
	"time"
)

type Block124Handler struct {
	Out chan<- model.SpProsesDpZscreg
}

func (h *Block124Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	NoVal, _ := strconv.Atoi(safe(fields, 3))
	LsnoVal, _ := strconv.Atoi(safe(fields, 4))
	DiscRegHdrQtyVal, _ := utils.ParseAccountingFloat(safe(fields, 5))
	AmountVal, _ := utils.ParseAccountingFloat(safe(fields, 6))
	now := time.Now()

	h.Out <- model.SpProsesDpZscreg{
		ProcessId:         processID,
		BlockId:           safe(fields, 0),
		BlockName:         safe(fields, 1),
		ConditionRecordNo: safe(fields, 2),
		No:                NoVal,
		Lsno:              LsnoVal,
		DiscRegHdrQty:     DiscRegHdrQtyVal,
		Amount:            AmountVal,
		Unit:              safe(fields, 7),
		FileName:          job.FileName,
		LineNumber:        lineNo,
		Cdate:             now,
	}

	return nil
}
