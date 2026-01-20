package worker

import (
	"go-import-file/internal/model"
	"time"
)

type Block111Handler struct {
	Out chan<- model.McustInvH
}

func (h *Block111Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.McustInvH{
		Bid:             safe(fields, 6),
		Bname:           safe(fields, 7),
		MuId:            safe(fields, 8),
		MuName:          safe(fields, 9),
		CustNo:          safe(fields, 10),
		CustName:        safe(fields, 11),
		InvTotal:        safe(fields, 12),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
