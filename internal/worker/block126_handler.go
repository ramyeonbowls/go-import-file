package worker

import (
	"go-import-file/internal/model"
	"strconv"
	"time"
)

type Block126Handler struct {
	Out chan<- model.SpProsesDpZ00001
}

func (h *Block126Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	ValidFromVal, _ := strconv.Atoi(safe(fields, 7))
	ValidToVal, _ := strconv.Atoi(safe(fields, 8))
	now := time.Now()

	h.Out <- model.SpProsesDpZ00001{
		ProcessId:     processID,
		BlockId:       safe(fields, 0),
		BlockName:     safe(fields, 1),
		Step:          safe(fields, 3),
		Counter:       safe(fields, 4),
		ConditionType: safe(fields, 5),
		Description:   safe(fields, 6),
		ValidFrom:     ValidFromVal,
		ValidTo:       ValidToVal,
		CondGrp:       safe(fields, 9),
		Drule:         safe(fields, 10),
		FileName:      job.FileName,
		LineNumber:    lineNo,
		Cdate:         now,
		DiscType:      safe(fields, 11),
	}

	return nil
}
