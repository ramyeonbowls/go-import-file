package model

import (
	"time"
)

type MkplPrice struct {
	UniqID          string
	LineNo          int
	CustCode        string
	Pcode           string
	PriceValue      float64
	PriceUom        string
	BranchID        string
	Cby             string
	Cdate           time.Time
	Mby             string
	Mdate           time.Time
	CoreFilename    string
	CoreProcessdate time.Time
}
