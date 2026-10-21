package pytime

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
)

var ErrTime = errors.New("unparseable timestamp")

type Clock interface {
	Now() time.Time
}

type Wall struct{}

func (Wall) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

type Fixed struct{ T time.Time }

func (f *Fixed) Now() time.Time { return f.T }

func ISO(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond()/1000 == 0 {
		return t.Format("2006-01-02T15:04:05Z")
	}
	return t.Format("2006-01-02T15:04:05.000000Z")
}

func FromTimestamp(x float64) time.Time {
	intpart, frac := math.Modf(x)
	frac *= 1e6
	frac = roundHalfEven(frac)
	if frac >= 1e6 {
		frac -= 1e6
		intpart += 1
	} else if frac < 0 {
		frac += 1e6
		intpart -= 1
	}
	return time.Unix(int64(intpart), int64(frac)*1000).UTC()
}

func roundHalfEven(x float64) float64 {
	r := math.Round(x)
	if math.Abs(x-r) == 0.5 {
		r = 2.0 * math.Round(x/2.0)
	}
	return r
}

func fromNumber(number float64) time.Time {
	if math.Abs(number) > 1e14 {
		number /= 1e6
	} else if math.Abs(number) > 1e11 {
		number /= 1e3
	}
	return FromTimestamp(number)
}

func isNumericText(s string) bool {
	s = strings.Replace(s, ".", "", 1)
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

var isoPattern = regexp.MustCompile(
	`^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2})(?::(\d{2})(?::(\d{2})(?:[.,](\d+))?)?)?)?(?:([+-])(\d{2}):?(\d{2})(?::?(\d{2}))?)?$`)

func parseISO(text string) (time.Time, error) {
	s := strings.ReplaceAll(text, "Z", "+00:00")
	m := isoPattern.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, fmt.Errorf("%w: %q", ErrTime, text)
	}
	num := func(v string) int {
		if v == "" {
			return 0
		}
		n, _ := strconv.Atoi(v)
		return n
	}
	micro := 0
	if m[7] != "" {
		frac := m[7]
		if len(frac) > 6 {
			frac = frac[:6]
		}
		for len(frac) < 6 {
			frac += "0"
		}
		micro = num(frac)
	}
	if num(m[2]) < 1 || num(m[2]) > 12 || num(m[3]) < 1 || num(m[3]) > 31 || num(m[4]) > 23 || num(m[5]) > 59 || num(m[6]) > 59 {
		return time.Time{}, fmt.Errorf("%w: %q", ErrTime, text)
	}
	t := time.Date(num(m[1]), time.Month(num(m[2])), num(m[3]), num(m[4]), num(m[5]), num(m[6]), micro*1000, time.UTC)
	if t.Day() != num(m[3]) {
		return time.Time{}, fmt.Errorf("%w: %q", ErrTime, text)
	}
	if m[8] != "" {
		offset := time.Duration(num(m[9]))*time.Hour + time.Duration(num(m[10]))*time.Minute + time.Duration(num(m[11]))*time.Second
		if m[8] == "+" {
			t = t.Add(-offset)
		} else {
			t = t.Add(offset)
		}
	}
	return t, nil
}

func Parse(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v.UTC(), nil
	case int:
		return fromNumber(float64(v)), nil
	case int64:
		return fromNumber(float64(v)), nil
	case float64:
		return fromNumber(v), nil
	case pydec.Dec:
		return fromNumber(v.Float64()), nil
	case json.Number:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %q", ErrTime, string(v))
		}
		return fromNumber(f), nil
	case string:
		if isNumericText(v) {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return time.Time{}, fmt.Errorf("%w: %q", ErrTime, v)
			}
			return fromNumber(f), nil
		}
		return parseISO(v)
	case nil:
		return time.Time{}, fmt.Errorf("%w: missing", ErrTime)
	}
	return time.Time{}, fmt.Errorf("%w: %T", ErrTime, value)
}

func MustParse(value any) time.Time {
	t, err := Parse(value)
	if err != nil {
		panic(err)
	}
	return t
}

func ISOOf(value any) (string, error) {
	t, err := Parse(value)
	if err != nil {
		return "", err
	}
	return ISO(t), nil
}

func MinuteFloor(t time.Time) time.Time { return t.UTC().Truncate(time.Minute) }

const (
	Exact          = "EXACT"
	CollectionOnly = "COLLECTION_ONLY"
	Bucket60s      = "BUCKET(60s)"
	Incomplete     = "INCOMPLETE"
)

var (
	bucketRe = regexp.MustCompile(`^BUCKET\(([^()]+)\)$`)
	sizeRe   = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)(us|ms|s|m|h|d|w)$`)
	legacy   = map[string]string{"HOUR": "1h", "DAY": "1d", "BUCKET_1H": "1h", "BUCKET_1D": "1d", "BUCKET_1M": "1m"}
	factors  = map[string]pydec.Dec{
		"us": pydec.MustParse("0.000001"), "ms": pydec.MustParse("0.001"), "s": pydec.FromInt(1),
		"m": pydec.FromInt(60), "h": pydec.FromInt(3600), "d": pydec.FromInt(86400), "w": pydec.FromInt(604800),
	}
)

type BucketSize string

func checkSize(value string) (string, error) {
	value = strings.TrimSpace(value)
	m := sizeRe.FindStringSubmatch(value)
	if m == nil || pydec.MustParse(m[1]).Sign() <= 0 {
		return "", errors.New("bucket size must be a positive duration with an explicit unit")
	}
	return value, nil
}

func duration(value string) pydec.Dec {
	m := sizeRe.FindStringSubmatch(value)
	return pydec.Mul(pydec.MustParse(m[1]), factors[m[2]], pydec.DefaultPrec)
}

func NormalizePrecision(precision string, bucket BucketSize) (string, error) {
	if precision == "" || precision == "UNKNOWN" {
		return "", nil
	}
	if precision == Exact || precision == CollectionOnly {
		if bucket != "" {
			return "", errors.New("non-bucket precision cannot carry a bucket size")
		}
		return precision, nil
	}
	embedded := legacy[precision]
	if m := bucketRe.FindStringSubmatch(precision); m != nil {
		size, err := checkSize(m[1])
		if err != nil {
			return "", err
		}
		embedded = size
	}
	if precision == "BUCKET" && bucket == "" {
		return "", nil
	}
	supplied := ""
	if bucket != "" {
		size, err := checkSize(string(bucket))
		if err != nil {
			return "", err
		}
		supplied = size
	}
	if embedded == "" && precision != "BUCKET" {
		return "", fmt.Errorf("unsupported timestamp precision: %s", precision)
	}
	if embedded != "" && supplied != "" && !duration(embedded).Equal(duration(supplied)) {
		return "", errors.New("conflicting bucket duration declarations")
	}
	if embedded != "" {
		return "BUCKET(" + embedded + ")", nil
	}
	return "BUCKET(" + supplied + ")", nil
}

func equivalent(a, b string) bool {
	if a == b {
		return true
	}
	ma, mb := bucketRe.FindStringSubmatch(a), bucketRe.FindStringSubmatch(b)
	if ma != nil && mb != nil {
		return duration(ma[1]).Equal(duration(mb[1]))
	}
	return false
}

type Timestamp struct {
	Value, Precision, Reason, BucketAnchor string
}

func none(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (t Timestamp) Fields(field string) map[string]any {
	out := map[string]any{field: none(t.Value), field + "_prec": none(t.Precision), field + "_prec_reason": none(t.Reason)}
	if t.BucketAnchor != "" {
		out[field+"_bucket_anchor"] = t.BucketAnchor
	}
	return out
}

func (t Timestamp) Into(record map[string]any, field string) {
	for k, v := range t.Fields(field) {
		record[k] = v
	}
}

func CollectionClock(t time.Time) Timestamp {
	return Timestamp{Value: ISO(t), Precision: CollectionOnly}
}

func str(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok && s != ""
}

func Propagate(record map[string]any, field string, aliases []string, bucket BucketSize, missingReason string) (Timestamp, error) {
	if missingReason == "" {
		missingReason = Incomplete
	}
	if bucket == "" {
		if size, ok := record[field+"_bucket_size_s"]; ok && size != nil {
			bucket = BucketSize(fmt.Sprint(size) + "s")
		}
	}
	raw := record[field]
	anchor := ""
	if a, ok := str(record[field+"_bucket_anchor"]); ok {
		anchor = a
	}
	names := []string{field + "_prec", field + "_precision"}
	for _, a := range aliases {
		dup := false
		for _, n := range names {
			if n == a {
				dup = true
			}
		}
		if !dup {
			names = append(names, a)
		}
	}
	var declarations []string
	reason := ""
	for _, name := range names {
		if v, ok := str(record[name]); ok {
			declarations = append(declarations, v)
		}
		if reason == "" {
			if r, ok := str(record[name+"_reason"]); ok {
				reason = r
			}
		}
	}
	var normalized []string
	for _, d := range declarations {
		n, err := NormalizePrecision(d, bucket)
		if err != nil {
			return Timestamp{}, err
		}
		normalized = append(normalized, n)
	}
	for _, n := range normalized[min(1, len(normalized)):] {
		if !equivalent(normalized[0], n) {
			return Timestamp{}, fmt.Errorf("conflicting timestamp precision declarations for %s", field)
		}
	}
	precision := ""
	if len(normalized) > 0 {
		precision = normalized[0]
	}
	if _, declared := record[field+"_bucket_anchor"]; anchor == "" && precision != "" && bucketRe.MatchString(precision) && declared {
		anchor = "UNSPECIFIED"
	}
	if raw == nil {
		if reason == "" {
			if r, ok := str(record[field+"_reason"]); ok {
				reason = r
			} else {
				reason = missingReason
			}
		}
		p := ""
		if precision == CollectionOnly {
			p = CollectionOnly
		}
		return Timestamp{Precision: p, Reason: reason}, nil
	}
	value, err := ISOOf(raw)
	if err != nil {
		return Timestamp{}, err
	}
	if precision != "" {
		reason = ""
	} else if reason == "" {
		reason = missingReason
	}
	return Timestamp{Value: value, Precision: precision, Reason: reason, BucketAnchor: anchor}, nil
}

func Fields(field string, value any, precision string) map[string]any {
	return FieldsBucket(field, value, precision, "")
}

func FieldsBucket(field string, value any, precision string, bucket BucketSize) map[string]any {
	record := map[string]any{field: nil, field + "_prec": none(precision)}
	if value != nil {
		record[field] = value
	}
	t, err := Propagate(record, field, nil, bucket, "")
	if err != nil {
		panic(err)
	}
	return t.Fields(field)
}
