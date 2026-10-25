package pyjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"
)

func EncodeString(value string, ascii bool) []byte {
	var buffer bytes.Buffer
	writeString(&buffer, value, ascii)
	return buffer.Bytes()
}

func ExactJSONNumber(raw []byte) ([]byte, error) {
	value := string(bytes.TrimSpace(raw))
	if value == "" || !json.Valid([]byte(value)) {
		return nil, errors.New("invalid raw JSON number")
	}
	if value[0] != '-' && (value[0] < '0' || value[0] > '9') {
		return nil, errors.New("raw JSON scalar is not a number")
	}
	if !strings.ContainsAny(value, ".eE") {
		integer, ok := new(big.Int).SetString(value, 10)
		if !ok {
			return nil, errors.New("invalid JSON integer")
		}
		return []byte(integer.String()), nil
	}
	negative := strings.HasPrefix(value, "-")
	magnitude := strings.TrimPrefix(value, "-")
	components := strings.FieldsFunc(magnitude, func(char rune) bool { return char == 'e' || char == 'E' })
	exponent := 0
	if len(components) == 2 {
		parsed, err := strconv.ParseInt(components[1], 10, 32)
		if err != nil {
			return nil, errors.New("JSON decimal exponent exceeds native numeric bounds")
		}
		exponent = int(parsed)
	}
	mantissa := components[0]
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		exponent -= len(mantissa) - dot - 1
		mantissa = mantissa[:dot] + mantissa[dot+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		if negative {
			return []byte("-0.0"), nil
		}
		return []byte("0.0"), nil
	}
	significant := strings.TrimRight(digits, "0")
	exponent += len(digits) - len(significant)
	digits = significant
	adjusted := len(digits) + exponent - 1
	var result string
	if adjusted >= 16 || adjusted < -4 {
		result = digits[:1]
		if len(digits) > 1 {
			result += "." + digits[1:]
		}
		sign := "+"
		power := adjusted
		if power < 0 {
			sign = "-"
			power = -power
		}
		suffix := strconv.Itoa(power)
		if len(suffix) < 2 {
			suffix = "0" + suffix
		}
		result += "e" + sign + suffix
	} else {
		position := len(digits) + exponent
		switch {
		case position <= 0:
			result = "0." + strings.Repeat("0", -position) + digits
		case position >= len(digits):
			result = digits + strings.Repeat("0", position-len(digits)) + ".0"
		default:
			result = digits[:position] + "." + digits[position:]
		}
	}
	if negative {
		result = "-" + result
	}
	return []byte(result), nil
}
