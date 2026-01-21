package model

import (
	"time"
)

type MTop struct {
	Top             string
	TopDesc         string
	TopDays         string
	CoreFilename    string
	CoreProcessdate time.Time
}
