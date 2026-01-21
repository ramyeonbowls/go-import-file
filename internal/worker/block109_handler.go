package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block109Handler struct {
	Out chan<- model.MShipTo
}

func (h *Block109Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MShipTo{
		CustNo:          safe(fields, 2),
		CustNoShip:      safe(fields, 3),
		DescCustNoShip:  safe(fields, 4),
		Kodecabang:      safe(fields, 5),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
