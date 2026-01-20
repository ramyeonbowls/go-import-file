package model

import (
	"time"
)

type SlsInv struct {
	SlsNo           string
	CustNo          string
	SfaOrderNo      string
	SfaOrderDate    string
	OrderNo         string
	OrderDate       string
	InvoiceNo       string
	InvoiceDate     string
	Pcode           string
	Qty             int
	Price           int
	Diskon          int
	Kodecabang      string
	InvType         string
	RefCn           string
	Invamount       int
	CoreFilename    string
	CoreProcessdate time.Time
}
