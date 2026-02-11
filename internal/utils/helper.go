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

func ParseAccountingInt(s string) (int, error) {
	s = strings.TrimSpace(s)

	negative := strings.HasSuffix(s, "-")

	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, "-", "")

	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if negative {
		v = -v
	}

	return v, nil
}

func ParseAccountingFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)

	if s == "" {
		return 0, nil
	}

	negative := false

	// Trailing minus
	if strings.HasSuffix(s, "-") {
		negative = true
		s = strings.TrimSuffix(s, "-")
	}

	// Leading minus
	if strings.HasPrefix(s, "-") {
		negative = true
		s = strings.TrimPrefix(s, "-")
	}

	hasDot := strings.Contains(s, ".")
	hasComma := strings.Contains(s, ",")

	// Case: 1.234,56 (EU)
	if hasDot && hasComma {
		s = strings.ReplaceAll(s, ".", "")
		s = strings.ReplaceAll(s, ",", ".")
	} else if hasDot {
		// Cuma ada dot
		parts := strings.Split(s, ".")

		// Kalau segment terakhir panjangnya 3 → thousand
		if len(parts[len(parts)-1]) == 3 {
			s = strings.ReplaceAll(s, ".", "")
		}
		// else → decimal, biarkan
	} else if hasComma {
		// Cuma ada comma → decimal EU
		s = strings.ReplaceAll(s, ",", ".")
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	if negative {
		v = -v
	}

	return v, nil
}
