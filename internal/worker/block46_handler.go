package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block46Handler struct {
	Out chan<- model.Mkat
}

func (h *Block46Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.Mkat{
		Kode:            safe(fields, 2),
		Ket:             safe(fields, 3),
		KodeDistributor: safe(fields, 4),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
