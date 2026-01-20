package model

import (
	"time"
)

type McustType struct {
	Type            string
	TypeName        string
	CoreFilename    string
	CoreProcessdate time.Time
}
