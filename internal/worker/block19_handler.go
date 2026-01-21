package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block19Handler struct {
	Out chan<- model.MRute
}

func (h *Block19Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.MRute{
		Region:          safe(fields, 2),
		Cabang:          safe(fields, 3),
		Kodecabang:      safe(fields, 4),
		SlsNo:           safe(fields, 5),
		NoRute:          safe(fields, 6),
		CustNo:          safe(fields, 7),
		HSatu:           safe(fields, 8),
		HDua:            safe(fields, 9),
		HTiga:           safe(fields, 10),
		HEmpat:          safe(fields, 11),
		HLima:           safe(fields, 12),
		HEnam:           safe(fields, 13),
		HTujuh:          safe(fields, 14),
		MSatu:           safe(fields, 15),
		MDua:            safe(fields, 16),
		MTiga:           safe(fields, 17),
		MEmpat:          safe(fields, 18),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
