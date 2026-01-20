package model

import (
	"time"
)

type Mprice struct {
	UniqID          string
	LineNo          int
	PriceCode       string
	BranchID        string
	Pcode           string
	PriceValue      string
	PriceUom        string
	Cby             string
	Cdate           time.Time
	Mby             string
	Mdate           time.Time
	CoreFilename    string
	CoreProcessdate time.Time
}
