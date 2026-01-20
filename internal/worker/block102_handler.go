package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block102Handler struct {
	Out chan<- model.MDistrict
}

func (h *Block102Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MDistrict{
		Distrik:         safe(fields, 4),
		DistrikName:     safe(fields, 3),
		KodeCabang:      safe(fields, 2),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
