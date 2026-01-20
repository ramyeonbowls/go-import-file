package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block112Handler struct {
	Out chan<- model.McustInvD
}

func (h *Block112Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	InvAmountVal, _ := utils.ParseNumber(safe(fields, 15))
	InvOutStandingVal, _ := utils.ParseNumber(safe(fields, 16))
	InvDateVal := safe(fields, 13)[:4] + "-" + safe(fields, 13)[4:6] + "-" + safe(fields, 13)[6:]
	DueDateVal := safe(fields, 14)[:4] + "-" + safe(fields, 14)[4:6] + "-" + safe(fields, 14)[6:]

	h.Out <- model.McustInvD{
		Bid:             safe(fields, 6),
		Bname:           safe(fields, 7),
		MuId:            safe(fields, 8),
		MuName:          safe(fields, 9),
		CustNo:          safe(fields, 10),
		CustName:        safe(fields, 11),
		InvNo:           safe(fields, 12),
		InvDate:         InvDateVal,
		DueDate:         DueDateVal,
		InvAmount:       InvAmountVal,
		InvOutStanding:  InvOutStandingVal,
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
