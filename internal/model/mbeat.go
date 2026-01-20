package model

import (
	"time"
)

type Mbeat struct {
	WcDistrictId    string
	WcWilayahId     string
	WcWilayahDesc   string
	CoreFilename    string
	CoreProcessdate time.Time
}
