package worker

import (
	"go-import-file/internal/model"

	"time"
)

type Block120Handler struct {
	Out chan<- model.SpProsesDpZdhdr
}

func (h *Block120Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	now := time.Now()

	ValidFrom := safe(fields, 13)
	ValidUntil := safe(fields, 12)

	Validf, _ := time.Parse("20060102", ValidFrom)
	Validt, _ := time.Parse("20060102", ValidUntil)

	ConditionTypeVal := safe(fields, 2)
	KeyCombinationVal := safe(fields, 3)
	SalesOrganizationVal := safe(fields, 4)
	DistributionChannelVal := safe(fields, 5)
	SalesOfficeVal := safe(fields, 6)
	DivisionVal := safe(fields, 7)
	PaymentTermVal := safe(fields, 8)
	CustomerVal := safe(fields, 9)
	Attribut2Val := safe(fields, 11)

	KeyComb := ConditionTypeVal + KeyCombinationVal + SalesOrganizationVal + DistributionChannelVal + SalesOfficeVal + DivisionVal + PaymentTermVal + CustomerVal + Attribut2Val

	h.Out <- model.SpProsesDpZdhdr{
		ProcessId:           processID,
		BlockId:             safe(fields, 0),
		BlockName:           safe(fields, 1),
		ConditionType:       ConditionTypeVal,
		Keycombination:      KeyCombinationVal,
		Keycomb:             KeyComb,
		SalesOrganization:   SalesOrganizationVal,
		DistributionChannel: DistributionChannelVal,
		SalesOffice:         SalesOfficeVal,
		Division:            DivisionVal,
		PaymentTerm:         PaymentTermVal,
		Customer:            CustomerVal,
		Material:            safe(fields, 10),
		Attribut2:           Attribut2Val,
		ValidUntil:          Validt,
		ValidFrom:           Validf,
		ConditionRecordno:   safe(fields, 14),
		Scale:               safe(fields, 15),
		FileName:            job.FileName,
		LineNumber:          lineNo,
		CDate:               now,
	}

	return nil
}
