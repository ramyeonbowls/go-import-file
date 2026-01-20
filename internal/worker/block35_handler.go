package worker

import (
	"strconv"
	"time"

	"go-import-file/internal/model"
)

type Block35Handler struct {
	Out chan<- model.ArInvoice
}

func (h *Block35Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	InvAmountVal, _ := strconv.ParseFloat(safe(fields, 6), 64)
	AmountPaidVal, _ := strconv.ParseFloat(safe(fields, 7), 64)

	h.Out <- model.ArInvoice{
		CustNo:          safe(fields, 2),
		InvNo:           safe(fields, 3),
		InvDate:         safe(fields, 4),
		DueDate:         safe(fields, 5),
		InvAmount:       InvAmountVal,
		AmountPaid:      AmountPaidVal,
		SlsNo:           safe(fields, 8),
		Kodecabang:      safe(fields, 9),
		InvType:         safe(fields, 10),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
