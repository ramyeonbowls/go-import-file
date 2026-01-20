package utils

import (
	"strconv"
	"strings"
)

func ParseNumber(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)

	// format: 1.234,56
	if strings.Contains(raw, ".") && strings.Contains(raw, ",") {
		raw = strings.ReplaceAll(raw, ".", "")
		raw = strings.ReplaceAll(raw, ",", ".")
		return strconv.ParseFloat(raw, 64)
	}

	// format: 35,000 (ribuan)
	if strings.Contains(raw, ",") {
		// cek apakah koma ribuan (3 digit di belakang)
		parts := strings.Split(raw, ",")
		if len(parts[len(parts)-1]) == 3 {
			raw = strings.ReplaceAll(raw, ",", "")
			return strconv.ParseFloat(raw, 64)
		}

		// format: 0,01 (desimal)
		raw = strings.ReplaceAll(raw, ",", ".")
	}

	return strconv.ParseFloat(raw, 64)
}
