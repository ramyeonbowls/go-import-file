package model

import (
	"time"
)

type McustGrp struct {
	GroupOut        string
	GroupName       string
	CoreFilename    string
	CoreProcessdate time.Time
}
