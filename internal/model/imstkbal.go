package model

import (
	"time"
)

type ImStkbal struct {
	Kg              string
	Pcode           string
	Stock           float64
	Kodecabang      string
	CoreFilename    string
	CoreProcessdate time.Time
}
