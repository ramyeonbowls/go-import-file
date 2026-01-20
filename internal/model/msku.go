package model

import "time"

type Msku struct {
	Prlin           string
	Brand           string
	Pcode           string
	Data1           string
	PcodeName       string
	Unit1           string
	Unit2           string
	Unit3           string
	Unit4           string
	Unit5           string
	Convunit2       int
	Convunit3       int
	Convunit4       int
	Convunit5       int
	Ppn             int
	FlagAktif       string
	FlagGift        string
	ShortName1      string
	FlagPkg         string
	UomBase         string
	UomMain         string
	Uom1Buy         string
	Uom2Buy         string
	Uom3Buy         string
	Uom4Buy         string
	Uom5Buy         string
	CoreFilename    string
	CoreProcessdate time.Time
}
