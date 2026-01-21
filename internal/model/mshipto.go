package model

import (
	"time"
)

type MShipTo struct {
	CustNo          string
	CustNoShip      string
	DescCustNoShip  string
	Kodecabang      string
	CoreFilename    string
	CoreProcessdate time.Time
}
