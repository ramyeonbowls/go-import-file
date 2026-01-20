package model

import (
	"time"
)

type Mmarket struct {
	PsrPasarId      string
	PsrLongDesc     string
	PsrShortDesc    string
	Kodecabang      string
	CoreFilename    string
	CoreProcessdate time.Time
}
