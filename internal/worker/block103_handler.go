package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block103Handler struct {
	Out chan<- model.Mbeat
}

func (h *Block103Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.Mbeat{
		WcDistrictId:    safe(fields, 2),
		WcWilayahId:     safe(fields, 3),
		WcWilayahDesc:   safe(fields, 4),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
