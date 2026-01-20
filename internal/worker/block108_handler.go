package worker

import (
	"go-import-file/internal/model"
	"time"
)

type Block108Handler struct {
	Out chan<- model.MBackOrder
}

func (h *Block108Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	TglOrderVal := safe(fields, 2)[:4] + "-" + safe(fields, 2)[4:6] + "-" + safe(fields, 2)[6:]

	h.Out <- model.MBackOrder{
		TglOrder:        TglOrderVal,
		OrderNo:         safe(fields, 3),
		SlsNo:           safe(fields, 4),
		CustNo:          safe(fields, 5),
		Kodecabang:      safe(fields, 6),
		OrderNoTopUp:    safe(fields, 7),
		Pcode:           safe(fields, 8),
		Status:          safe(fields, 9),
		StatusDetail:    safe(fields, 10),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
