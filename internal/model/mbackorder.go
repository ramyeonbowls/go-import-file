package model

import (
	"time"
)

type MBackOrder struct {
	TglOrder        string
	OrderNo         string
	SlsNo           string
	CustNo          string
	Kodecabang      string
	OrderNoTopUp    string
	Pcode           string
	Status          string
	StatusDetail    string
	CoreFilename    string
	CoreProcessdate time.Time
}
