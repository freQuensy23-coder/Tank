package pyjson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

func Dumps(value any, ensureASCII bool) ([]byte, error) {
	var b bytes.Buffer
	if err := encode(&b, value, ensureASCII); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func MustDumps(value any, ensureASCII bool) []byte {
	out, err := Dumps(value, ensureASCII)
	if err != nil {
		panic(err)
	}
	return out
}

func Canonical(value any) string { return string(MustDumps(value, false)) }

func SHA256(value any) string {
	sum := sha256.Sum256(MustDumps(value, false))
	return hex.EncodeToString(sum[:])
}

func encode(b *bytes.Buffer, value any, ascii bool) error {
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if v {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		writeString(b, v, ascii)
	case int:
		b.WriteString(strconv.Itoa(v))
	case int64:
		b.WriteString(strconv.FormatInt(v, 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case uint64:
		b.WriteString(strconv.FormatUint(v, 10))
	case float64:
		s, err := FloatRepr(v)
		if err != nil {
			return err
		}
		b.WriteString(s)
	case json.Number:

		if isInteger(string(v)) {
			b.WriteString(strings.TrimPrefix(string(v), "+"))
		} else {
			d, err := pydec.Parse(string(v))
			if err != nil {
				return err
			}
			writeString(b, d.String(), ascii)
		}
	case pydec.Dec:
		writeString(b, v.String(), ascii)
	case time.Time:
		writeString(b, pytime.ISO(v), ascii)
	case []any:
		if v == nil {
			b.WriteString("null")
			break
		}
		b.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := encode(b, item, ascii); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case []string:
		if v == nil {
			b.WriteString("null")
			break
		}
		b.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, item, ascii)
		}
		b.WriteByte(']')
	case map[string]any:
		if v == nil {
			b.WriteString("null")
			break
		}
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, k, ascii)
			b.WriteByte(':')
			if err := encode(b, v[k], ascii); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("pyjson: unsupported value %T", value)
	}
	return nil
}

func isInteger(s string) bool {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

const hexDigits = "0123456789abcdef"

func writeString(b *bytes.Buffer, s string, ascii bool) {
	b.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			switch {
			case r < 0x20 || (ascii && r == 0x7f):
				writeU(b, r)
			case ascii && r > 0x7f:
				if r > 0xffff {
					r -= 0x10000
					writeU(b, 0xd800|(r>>10)&0x3ff)
					writeU(b, 0xdc00|r&0x3ff)
				} else {
					writeU(b, r)
				}
			default:
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

func writeU(b *bytes.Buffer, r rune) {
	b.WriteString(`\u`)
	b.WriteByte(hexDigits[(r>>12)&0xf])
	b.WriteByte(hexDigits[(r>>8)&0xf])
	b.WriteByte(hexDigits[(r>>4)&0xf])
	b.WriteByte(hexDigits[r&0xf])
}

func FloatRepr(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("pyjson: out of range float %v", f)
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0", nil
		}
		return "0.0", nil
	}
	sci := strconv.FormatFloat(f, 'e', -1, 64)
	sign := ""
	if sci[0] == '-' {
		sign, sci = "-", sci[1:]
	}
	mant, expText, _ := strings.Cut(sci, "e")
	exp, _ := strconv.Atoi(expText)
	digits := strings.Replace(mant, ".", "", 1)
	decpt := exp + 1
	if decpt <= -4 || decpt > 16 {
		out := digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		e := decpt - 1
		esign := "+"
		if e < 0 {
			esign, e = "-", -e
		}
		es := strconv.Itoa(e)
		if len(es) < 2 {
			es = "0" + es
		}
		return sign + out + "e" + esign + es, nil
	}
	switch {
	case decpt <= 0:
		return sign + "0." + strings.Repeat("0", -decpt) + digits, nil
	case decpt >= len(digits):
		return sign + digits + strings.Repeat("0", decpt-len(digits)) + ".0", nil
	default:
		return sign + digits[:decpt] + "." + digits[decpt:], nil
	}
}
