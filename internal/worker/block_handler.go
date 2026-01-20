package worker

import "go-import-file/internal/model"

type BlockHandler interface {
	Handle(
		fields []string,
		lineNo int,
		job FileJob,
		processID string,
	) error
}

func BuildBlockHandlers(
	ch16 chan<- model.Mprice,
	ch15 chan<- model.MpriceGrp,
	ch01 chan<- model.Mcust,
	ch25 chan<- model.Msku,
	ch02 chan<- model.McustGrp,
	ch05 chan<- model.McustIndus,
	ch20 chan<- model.Msalesman,
	ch43 chan<- model.SlsInv,
	ch35 chan<- model.ArInvoice,
	ch39 chan<- model.ImStkbal,
	ch108 chan<- model.MBackOrder,
	ch103 chan<- model.Mbeat,
	ch44 chan<- model.McustCl,
	ch112 chan<- model.McustInvD,
	ch111 chan<- model.McustInvH,
	ch03 chan<- model.McustType,
) map[string]BlockHandler {

	return map[string]BlockHandler{
		"16":  &Block16Handler{Out: ch16},
		"15":  &Block15Handler{Out: ch15},
		"01":  &Block01Handler{Out: ch01},
		"25":  &Block25Handler{Out: ch25},
		"02":  &Block02Handler{Out: ch02},
		"05":  &Block05Handler{Out: ch05},
		"20":  &Block20Handler{Out: ch20},
		"43":  &Block43Handler{Out: ch43},
		"35":  &Block35Handler{Out: ch35},
		"39":  &Block39Handler{Out: ch39},
		"108": &Block108Handler{Out: ch108},
		"103": &Block103Handler{Out: ch103},
		"44":  &Block44Handler{Out: ch44},
		"112": &Block112Handler{Out: ch112},
		"111": &Block111Handler{Out: ch111},
		"03":  &Block03Handler{Out: ch03},
	}
}
