package worker

import (
	"go-import-file/internal/model"

	"time"
)

type Block121Handler struct {
	Out chan<- model.SpProsesDpZditm
}

func (h *Block121Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	now := time.Now()

	ValidFrom := safe(fields, 18)
	ValidUntil := safe(fields, 19)

	Validf, _ := time.Parse("20060102", ValidFrom)
	Validt, _ := time.Parse("20060102", ValidUntil)

	ConditionTypeVal := safe(fields, 2)
	KeyCombinationVal := safe(fields, 3)
	SalesOrganizationVal := safe(fields, 4)
	DistributionChannelVal := safe(fields, 5)
	SalesOfficeVal := safe(fields, 6)
	DivisionVal := safe(fields, 7)
	SoldToPartyVal := safe(fields, 8)
	PricingRefMatlVal := safe(fields, 9)
	PaymentTermsVal := safe(fields, 10)
	IndustryCode3Val := safe(fields, 11)
	IndustryCode4Val := safe(fields, 12)
	IndustryCode5Val := safe(fields, 13)
	Attribute1Val := safe(fields, 14)
	Attribute2Val := safe(fields, 15)
	SalesUnitVal := safe(fields, 17)

	KeyComb := ConditionTypeVal + KeyCombinationVal + SalesOrganizationVal + DistributionChannelVal + SalesOfficeVal + DivisionVal + SoldToPartyVal + PricingRefMatlVal + PaymentTermsVal + IndustryCode3Val + IndustryCode4Val + IndustryCode5Val + Attribute1Val + Attribute2Val + SalesUnitVal

	h.Out <- model.SpProsesDpZditm{
		ProcessId:           processID,
		BlockId:             safe(fields, 0),
		BlockName:           safe(fields, 1),
		ConditionType:       ConditionTypeVal,
		KeyCombination:      KeyCombinationVal,
		KeyComb:             KeyComb,
		SalesOrganization:   SalesOrganizationVal,
		DistributionChannel: DistributionChannelVal,
		SalesOffice:         SalesOfficeVal,
		Division:            DivisionVal,
		SoldToParty:         SoldToPartyVal,
		PricingRefMatl:      PricingRefMatlVal,
		PaymentTerms:        PaymentTermsVal,
		IndustryCode3:       IndustryCode3Val,
		IndustryCode4:       IndustryCode4Val,
		IndustryCode5:       IndustryCode5Val,
		Attribute1:          Attribute1Val,
		Attribute2:          Attribute2Val,
		Material:            safe(fields, 16),
		SalesUnit:           SalesUnitVal,
		ValidFrom:           Validf,
		ValidUntil:          Validt,
		ConditionRecordNo:   safe(fields, 20),
		Scale:               safe(fields, 21),
		FileName:            job.FileName,
		LineNumber:          lineNo,
		CDate:               now,
	}

	return nil
}
