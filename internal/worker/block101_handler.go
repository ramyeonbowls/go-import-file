package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block101Handler struct {
	Out chan<- model.MProvince
}

func (h *Block101Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MProvince{
		ProvinsiId:      safe(fields, 2),
		ProvinsiName:    safe(fields, 3),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
