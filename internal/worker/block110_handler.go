package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block110Handler struct {
	Out chan<- model.MPayerTo
}

func (h *Block110Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MPayerTo{
		CustNo:          safe(fields, 2),
		CustNoBil:       safe(fields, 3),
		DescCustNoBil:   safe(fields, 4),
		Kodecabang:      safe(fields, 5),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
