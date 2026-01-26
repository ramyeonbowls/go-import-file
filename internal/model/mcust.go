package model

import "time"

type Mcust struct {
	Custno          string
	Data01          string
	CustName        string
	CustAdd1        string
	CustAdd2        string
	City            string
	Contact         string
	Phone1          string
	FaxNo           string
	Cterm           string
	Climit          int
	FlagLimit       string
	Gdisc           string
	GrupOut         string
	TypeOut         string
	Gharga          string
	FlagPay         string
	FlagOut         string
	Rpp             int
	Lsales          int
	Ldatetrs        string
	Lokasi          string
	Distrik         string
	Beat            string
	SubBeat         string
	Klasif          string
	Kindus          string
	Kpasar          string
	BranchID        string
	La              string
	Lg              string
	CoreFilename    string
	CoreProcessdate time.Time
}
