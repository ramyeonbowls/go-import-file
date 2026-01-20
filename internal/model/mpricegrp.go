package model

import (
	"time"
)

type MpriceGrp struct {
	PriceCode       string
	PriceDesc       string
	CoreFilename    string
	CoreProcessdate time.Time
}
