package worker

import (
	"strconv"
	"strings"
	"time"

	"go-import-file/internal/model"
)

type Block43Handler struct {
	Out chan<- model.SlsInv
}

func (h *Block43Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {

	QtyVal, _ := strconv.Atoi(safe(fields, 11))
	PriceCVal := strings.ReplaceAll(safe(fields, 12), ",", "")
	PriceVal, _ := strconv.Atoi(PriceCVal)
	DiskonCval := strings.ReplaceAll(safe(fields, 13), ",", "")
	DiskonVal, _ := strconv.Atoi(DiskonCval)
	InvamountCVal := strings.ReplaceAll(safe(fields, 17), ",", "")
	InvamountVal, _ := strconv.Atoi(InvamountCVal)
	SfaOrderDateVal := safe(fields, 5)[:4] + "-" + safe(fields, 5)[4:6] + "-" + safe(fields, 5)[6:]
	OrderDateVal := safe(fields, 7)[:4] + "-" + safe(fields, 7)[4:6] + "-" + safe(fields, 7)[6:]
	InvoiceDateVal := safe(fields, 9)[:4] + "-" + safe(fields, 9)[4:6] + "-" + safe(fields, 9)[6:]

	h.Out <- model.SlsInv{
		SlsNo:           safe(fields, 2),
		CustNo:          safe(fields, 3),
		SfaOrderNo:      safe(fields, 4),
		SfaOrderDate:    SfaOrderDateVal,
		OrderNo:         safe(fields, 6),
		OrderDate:       OrderDateVal,
		InvoiceNo:       safe(fields, 8),
		InvoiceDate:     InvoiceDateVal,
		Pcode:           safe(fields, 10),
		Qty:             QtyVal,
		Price:           PriceVal,
		Diskon:          DiskonVal,
		Kodecabang:      safe(fields, 14),
		InvType:         safe(fields, 15),
		RefCn:           safe(fields, 16),
		Invamount:       InvamountVal,
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}
