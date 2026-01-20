package model

import (
	"time"
)

type ArInvoice struct {
	CustNo          string
	InvNo           string
	InvDate         string
	DueDate         string
	InvAmount       float64
	AmountPaid      float64
	SlsNo           string
	Kodecabang      string
	InvType         string
	CoreFilename    string
	CoreProcessdate time.Time
}
