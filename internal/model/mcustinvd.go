package model

import (
	"time"
)

type McustInvD struct {
	Bid             string
	Bname           string
	MuId            string
	MuName          string
	CustNo          string
	CustName        string
	InvNo           string
	InvDate         string
	DueDate         string
	InvAmount       float64
	InvOutStanding  float64
	CoreFilename    string
	CoreProcessdate time.Time
}
