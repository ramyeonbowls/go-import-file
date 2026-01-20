package model

import (
	"time"
)

type Msalesman struct {
	SlsNo           string
	SlsName         string
	Alamat1         string
	Alamat2         string
	Kota            string
	Pendidikan      string
	TglLahir        string
	TglMasuk        string
	TglTrans        string
	SlsPass         string
	Ec1             string
	Item            string
	Kodecabang      string
	AtasanId        string
	CoreFilename    string
	CoreProcessdate time.Time
}
