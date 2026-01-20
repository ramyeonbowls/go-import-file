package worker

import (
	"go-import-file/internal/model"
	"time"
)

type Block01Handler struct {
	Out chan<- model.Mcust
}

func (h *Block01Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	h.Out <- model.Mcust{
		Custno:          safe(fields, 2),
		Data01:          safe(fields, 3),
		CustName:        safe(fields, 4),
		CustAdd1:        safe(fields, 5),
		CustAdd2:        safe(fields, 6),
		City:            safe(fields, 7),
		Contact:         safe(fields, 8),
		Phone1:          safe(fields, 9),
		FaxNo:           safe(fields, 10),
		Cterm:           safe(fields, 11),
		Climit:          safe(fields, 12),
		FlagLimit:       safe(fields, 13),
		Gdisc:           safe(fields, 14),
		GrupOut:         safe(fields, 15),
		TypeOut:         safe(fields, 16),
		Gharga:          safe(fields, 17),
		FlagPay:         safe(fields, 18),
		FlagOut:         safe(fields, 19),
		Rpp:             safe(fields, 20),
		Lsales:          safe(fields, 21),
		Ldatetrs:        safe(fields, 22),
		Lokasi:          safe(fields, 23),
		Distrik:         safe(fields, 24),
		Beat:            safe(fields, 25),
		SubBeat:         safe(fields, 26),
		Klasif:          safe(fields, 27),
		Kindus:          safe(fields, 28),
		Kpasar:          safe(fields, 29),
		BranchID:        safe(fields, 30),
		La:              safe(fields, 31),
		Lg:              safe(fields, 32),
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
