package model

import (
	"time"
)

type MPayerTo struct {
	CustNo          string
	CustNoBil       string
	DescCustNoBil   string
	Kodecabang      string
	CoreFilename    string
	CoreProcessdate time.Time
}
