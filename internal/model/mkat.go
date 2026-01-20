package model

import (
	"time"
)

type Mkat struct {
	Kode            string
	Ket             string
	KodeDistributor string
	CoreFilename    string
	CoreProcessdate time.Time
}
