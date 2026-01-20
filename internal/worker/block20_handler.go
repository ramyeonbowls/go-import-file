package worker

import (
	"time"

	"go-import-file/internal/model"
)

type Block20Handler struct {
	Out chan<- model.Msalesman
}

func (h *Block20Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.Msalesman{
		SlsNo:           safe(fields, 2),
		SlsName:         safe(fields, 3),
		Alamat1:         safe(fields, 4),
		Alamat2:         safe(fields, 5),
		Kota:            safe(fields, 6),
		Pendidikan:      safe(fields, 7),
		TglLahir:        safe(fields, 8),
		TglMasuk:        safe(fields, 9),
		TglTrans:        safe(fields, 14),
		SlsPass:         safe(fields, 17),
		Ec1:             safe(fields, 18),
		Item:            safe(fields, 19),
		Kodecabang:      safe(fields, 20),
		AtasanId:        safe(fields, 21),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
