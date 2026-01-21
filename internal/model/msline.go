package model

import (
	"time"
)

type MSline struct {
	Prlin           string
	PrliName        string
	KompFlag        string
	CoreFilename    string
	CoreProcessdate time.Time
}
