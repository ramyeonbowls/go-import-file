package worker

import (
	"go-import-file/internal/model"
	"strconv"
	"time"
)

type Block123Handler struct {
	OutZpmix chan<- model.SpProsesDpZpmix
	OutFg    chan<- model.SpProsesDpZpmix
}

func (h *Block123Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	now := time.Now()

	ValidFrom := safe(fields, 17)
	ValidUntil := safe(fields, 16)

	Validf, _ := time.Parse("20060102", ValidFrom)
	Validt, _ := time.Parse("20060102", ValidUntil)

	LineItemVal, _ := strconv.Atoi(safe(fields, 19))
	VKelipatanVal, _ := strconv.Atoi(safe(fields, 27))
	VPerbandingan1Val, _ := strconv.Atoi(safe(fields, 29))
	VPerbandingan2Val, _ := strconv.Atoi(safe(fields, 30))

	data := model.SpProsesDpZpmix{
		ProcessId:      processID,
		BlockId:        safe(fields, 0),
		Blockname:      safe(fields, 1),
		Ctyp:           safe(fields, 2),
		KeyCombination: safe(fields, 3),
		Sorg:           safe(fields, 4),
		Dchl:           safe(fields, 5),
		Soff:           safe(fields, 6),
		Dv:             safe(fields, 7),
		Customer:       safe(fields, 8),
		Indcode2:       safe(fields, 9),
		Indcode3:       safe(fields, 10),
		Indcode4:       safe(fields, 11),
		Indcode5:       safe(fields, 12),
		Pl:             safe(fields, 13),
		Payt:           safe(fields, 14),
		Material:       safe(fields, 15),
		ValidFrom:      Validf,
		ValidUntil:     Validt,
		PromoId:        safe(fields, 18),
		LineItem:       LineItemVal,
		FileName:       job.FileName,
		LineNumber:     lineNo,
		Cdate:          now,
		MustBuy:        safe(fields, 20),
		Exclude:        safe(fields, 21),
		Split:          safe(fields, 22),
		Amountx:        safe(fields, 23),
		Rangex:         safe(fields, 24),
		WithMaterial:   safe(fields, 25),
		Kelipatan:      safe(fields, 26),
		VKelipatan:     VKelipatanVal,
		AttrPrdLv2:     safe(fields, 31),
		AttrPrdLv3:     safe(fields, 32),
		FlCustExc:      safe(fields, 33),
		CustExc:        safe(fields, 34),
		FlHd:           safe(fields, 35),
		Perbandingan:   safe(fields, 28),
		VPerbandingan1: VPerbandingan1Val,
		VPerbandingan2: VPerbandingan2Val,
	}

	h.OutZpmix <- data
	h.OutFg <- data

	return nil
}
