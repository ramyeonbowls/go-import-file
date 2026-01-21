package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block104Handler struct {
	Out chan<- model.MSubBeat
}

func (h *Block104Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MSubBeat{
		RcDistrictId:    safe(fields, 2),
		RcWilayahId:     safe(fields, 3),
		RcRayonId:       safe(fields, 4),
		RcRayonDesc:     safe(fields, 5),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
