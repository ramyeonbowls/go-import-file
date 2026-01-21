package model

import (
	"time"
)

type MSubBeat struct {
	RcDistrictId    string
	RcWilayahId     string
	RcRayonId       string
	RcRayonDesc     string
	CoreFilename    string
	CoreProcessdate time.Time
}
