package model

import (
	"time"
)

type McustInvH struct {
	Bid             string
	Bname           string
	MuId            string
	MuName          string
	CustNo          string
	CustName        string
	InvTotal        string
	CoreFilename    string
	CoreProcessdate time.Time
}
