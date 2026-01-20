package worker

import (
	"go-import-file/internal/config"
	"go-import-file/internal/model"
	"strconv"
	"strings"
	"time"
)

type Block25Handler struct {
	Out chan<- model.Msku
}

func (h *Block25Handler) Handle(
	fields []string,
	lineNo int,
	job FileJob,
	processID string,
) error {
	cfg := config.Load()

	uomFlags := [5]string{}

	parts := strings.Split(cfg.UomBuy, "|")

	for i := range len(uomFlags) {
		if safe(parts, i) != "" {
			uomFlags[i] = "Y"
		}
	}

	ConvUnit := safe(fields, 7) + "|" + safe(fields, 8) + "|" + safe(fields, 9) + "|" + safe(fields, 10) + "|" + safe(fields, 11)

	UomBuy1 := uomFlags[0]
	UomBuy2 := uomFlags[1]
	UomBuy3 := uomFlags[2]
	UomBuy4 := uomFlags[3]
	UomBuy5 := uomFlags[4]

	uom, pos := ResolveUOM(cfg.UomMain, ConvUnit)

	h.Out <- model.Msku{
		Prlin:           safe(fields, 2),
		Brand:           safe(fields, 3),
		Pcode:           safe(fields, 4),
		Data1:           safe(fields, 5),
		PcodeName:       safe(fields, 6),
		Unit1:           safe(fields, 7),
		Unit2:           safe(fields, 8),
		Unit3:           safe(fields, 9),
		Unit4:           safe(fields, 10),
		Unit5:           safe(fields, 11),
		Convunit2:       toInt(safe(fields, 12)),
		Convunit3:       toInt(safe(fields, 13)),
		Convunit4:       toInt(safe(fields, 14)),
		Convunit5:       toInt(safe(fields, 15)),
		Ppn:             toInt(safe(fields, 16)),
		FlagAktif:       safe(fields, 17),
		FlagGift:        safe(fields, 26),
		ShortName1:      safe(fields, 28),
		UomBase:         uom,
		UomMain:         strconv.Itoa(pos + 1),
		Uom1Buy:         UomBuy1,
		Uom2Buy:         UomBuy2,
		Uom3Buy:         UomBuy3,
		Uom4Buy:         UomBuy4,
		Uom5Buy:         UomBuy5,
		CoreFilename:    job.FileName,
		CoreProcessdate: time.Now(),
	}

	return nil
}

func toInt(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return val
}

func ResolveUOM(uomMain, convUnit string) (value string, position int) {
	convParts := strings.Split(convUnit, "|")
	convSet := make(map[string]int)
	lastValid := ""
	lastIndex := -1

	// Bangun lookup + simpan index
	for i, c := range convParts {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, exists := convSet[c]; !exists {
			convSet[c] = i
		}
		lastValid = c
		lastIndex = i
	}

	// Cek berdasarkan prioritas UOM_MAIN
	for _, m := range strings.Split(uomMain, "|") {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if idx, ok := convSet[m]; ok {
			return m, idx
		}
	}

	// Fallback: ambil nilai terakhir dari CONV_UNIT
	return lastValid, lastIndex
}
