package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"strconv"

	"time"
)

type Block130Handler struct {
	OutZdhdr chan<- model.SpProsesFgZdhdr
	OutFg    chan<- model.SpProsesFgZdhdr
}

func (h *Block130Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	now := time.Now()

	ValidFrom := safe(fields, 15)
	ValidUntil := safe(fields, 16)

	Validf, _ := time.Parse("20060102", ValidFrom)
	Validt, _ := time.Parse("20060102", ValidUntil)

	ConditionTypeVal := safe(fields, 2)
	if ConditionTypeVal == "" {
		ConditionTypeVal = "ZNA0"
	}
	KeyCombinationVal := safe(fields, 3)
	SalesOrganizationVal := safe(fields, 4)
	DistributionChannelVal := safe(fields, 5)
	DivisionVal := safe(fields, 6)
	SalesOfficeVal := safe(fields, 7)
	PricelistTypeVal := safe(fields, 8)
	Attribute1Val := safe(fields, 9)
	IndustryCode3Val := safe(fields, 10)
	IndustryCode4Val := safe(fields, 11)
	IndustryCode5Val := safe(fields, 12)
	SoldToPartyVal := safe(fields, 13)

	FKelipatanVal, _ := strconv.Atoi(safe(fields, 23))
	QtyVal, _ := strconv.Atoi(safe(fields, 28))
	UomVal, _ := utils.ParseNumber(safe(fields, 29))
	FPerbandingan1Val, _ := strconv.Atoi(safe(fields, 25))
	FPerbandingan2Val, _ := strconv.Atoi(safe(fields, 26))

	KeyComb := ConditionTypeVal + KeyCombinationVal + SalesOrganizationVal + DistributionChannelVal + DivisionVal + SalesOfficeVal + PricelistTypeVal + Attribute1Val + IndustryCode3Val + IndustryCode4Val + IndustryCode5Val + SoldToPartyVal

	data := model.SpProsesFgZdhdr{
		ProcessId:           processID,
		BlockId:             safe(fields, 0),
		BlockName:           safe(fields, 1),
		ConditionType:       safe(fields, 2),
		KeyCombination:      safe(fields, 3),
		KeyComb:             KeyComb,
		SalesOrganization:   safe(fields, 4),
		DistributionChannel: safe(fields, 5),
		Division:            safe(fields, 6),
		SalesOffice:         safe(fields, 7),
		PricelistType:       safe(fields, 8),
		Attribute1:          safe(fields, 9),
		IndustryCode3:       safe(fields, 10),
		IndustryCode4:       safe(fields, 11),
		IndustryCode5:       safe(fields, 12),
		SoldToParty:         safe(fields, 13),
		Material:            safe(fields, 14),
		ValidUntil:          Validf,
		ValidFrom:           Validt,
		ConditionRecordNo:   safe(fields, 17),
		PromoId:             safe(fields, 18),
		PromoItem:           safe(fields, 19),
		Scale:               safe(fields, 20),
		FileName:            job.FileName,
		LineNumber:          lineNo,
		CDate:               now,
		MustBuy:             safe(fields, 21),
		Kelipatan:           safe(fields, 22),
		FKelipatan:          FKelipatanVal,
		Perbandingan:        safe(fields, 24),
		FPerbandingan1:      FPerbandingan1Val,
		FPerbandingan2:      FPerbandingan2Val,
		WithQty:             safe(fields, 27),
		Qty:                 QtyVal,
		Uom:                 UomVal,
		Zterm:               safe(fields, 30),
		Katr2:               safe(fields, 31),
		Katr3:               safe(fields, 32),
		Amountx:             safe(fields, 33),
	}

	h.OutZdhdr <- data
	h.OutFg <- data

	return nil
}
