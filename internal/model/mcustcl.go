package model

import (
	"time"
)

type McustCl struct {
	CustNo          string
	CustName        string
	CreditLimit     float64
	SisaCreditLimit float64
	Kodecabang      string
	CoreFilename    string
	CoreProcessdate time.Time
}
