package worker

import (
	"go-import-file/internal/model"
	"go-import-file/internal/utils"
	"time"
)

type Block131Handler struct {
	Out chan<- model.SpProsesFgZfrdet
}

func (h *Block131Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	MinimumQtyVal, _ := utils.ParseNumber(safe(fields, 3))
	FreeGoodsQtyVal, _ := utils.ParseNumber(safe(fields, 4))
	FreeGoodsAgrredQtyVal, _ := utils.ParseNumber(safe(fields, 6))
	now := time.Now()

	h.Out <- model.SpProsesFgZfrdet{
		ProcessId:          processID,
		BlockId:            safe(fields, 0),
		BlockName:          safe(fields, 1),
		ConditionRecordNo:  safe(fields, 2),
		MinimumQty:         MinimumQtyVal,
		FreeGoodsQty:       FreeGoodsQtyVal,
		UomFreeGoods:       safe(fields, 5),
		FreeGoodsAgrredQty: FreeGoodsAgrredQtyVal,
		UomFreeGoodsAgrred: safe(fields, 7),
		AdditionalMaterial: safe(fields, 8),
		FileName:           job.FileName,
		LineNumber:         lineNo,
		CDate:              now,
	}

	return nil
}
