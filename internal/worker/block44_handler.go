package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block44Handler struct {
	Out chan<- model.McustCl
}

func (h *Block44Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	CreditLimitVal, _ := utils.ParseAccountingFloat(safe(fields, 4))
	SisaCreditLimitVal, _ := utils.ParseAccountingFloat(safe(fields, 5))

	h.Out <- model.McustCl{
		CustNo:          safe(fields, 2),
		CustName:        safe(fields, 3),
		CreditLimit:     CreditLimitVal,
		SisaCreditLimit: SisaCreditLimitVal,
		Kodecabang:      safe(fields, 6),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
