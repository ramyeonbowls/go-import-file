package model

import (
	"time"
)

type MRute struct {
	Region          string
	Cabang          string
	Kodecabang      string
	SlsNo           string
	NoRute          string
	CustNo          string
	HSatu           string
	HDua            string
	HTiga           string
	HEmpat          string
	HLima           string
	HEnam           string
	HTujuh          string
	MSatu           string
	MDua            string
	MTiga           string
	MEmpat          string
	CoreFilename    string
	CoreProcessdate time.Time
}
