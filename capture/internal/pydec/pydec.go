package pydec

import (
	"errors"
	"math/big"
	"strings"
)

const (
	DefaultPrec = 28
	Precise     = 100
)

var (
	ErrInvalid        = errors.New("invalid decimal literal")
	ErrNonFinite      = errors.New("nonfinite decimal")
	ErrDivisionByZero = errors.New("decimal division by zero")
)

type Dec struct {
	neg bool
	c   *big.Int
	exp int
}

var (
	bigTen  = big.NewInt(10)
	bigFive = big.NewInt(5)

	pow10 = func() []*big.Int {
		table := make([]*big.Int, 512)
		table[0] = big.NewInt(1)
		for i := 1; i < len(table); i++ {
			table[i] = new(big.Int).Mul(table[i-1], bigTen)
		}
		return table
	}()
)

func powTen(n int) *big.Int {
	if n < len(pow10) {
		return pow10[n]
	}
	return new(big.Int).Exp(bigTen, big.NewInt(int64(n)), nil)
}

func numDigits(c *big.Int) int {
	if c == nil || c.Sign() == 0 {
		return 1
	}
	if c.IsUint64() {
		v := c.Uint64()
		n := 1
		for v >= 10 {
			v /= 10
			n++
		}
		return n
	}

	n := int(float64(c.BitLen())*0.30102999566398120) + 1
	if n < 1 {
		n = 1
	}
	for c.Cmp(powTen(n)) >= 0 {
		n++
	}
	for n > 1 && c.Cmp(powTen(n-1)) < 0 {
		n--
	}
	return n
}

func mk(neg bool, c *big.Int, exp int) Dec {
	if c != nil && c.Sign() == 0 {
		c = nil
	}
	return Dec{neg: neg, c: c, exp: exp}
}

func FromInt(v int64) Dec {
	if v < 0 {
		return mk(true, new(big.Int).Neg(big.NewInt(v)), 0)
	}
	return mk(false, big.NewInt(v), 0)
}

var Zero = Dec{}

func Parse(text string) (Dec, error) {
	s := strings.TrimSpace(text)
	if strings.Contains(s, "_") {

		if strings.HasPrefix(s, "_") || strings.HasSuffix(s, "_") || strings.Contains(s, "__") {
			return Dec{}, ErrInvalid
		}
		s = strings.ReplaceAll(s, "_", "")
	}
	if s == "" {
		return Dec{}, ErrInvalid
	}
	neg := false
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		neg = true
		s = s[1:]
	}
	lower := strings.ToLower(s)
	if lower == "inf" || lower == "infinity" || strings.HasPrefix(lower, "nan") || strings.HasPrefix(lower, "snan") {
		return Dec{}, ErrNonFinite
	}
	mantissa, exponent := s, ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mantissa, exponent = s[:i], s[i+1:]
		if exponent == "" {
			return Dec{}, ErrInvalid
		}
	}
	intPart, fracPart := mantissa, ""
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		intPart, fracPart = mantissa[:i], mantissa[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return Dec{}, ErrInvalid
	}
	digits := intPart + fracPart
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return Dec{}, ErrInvalid
		}
	}
	exp := 0
	if exponent != "" {
		sign := 1
		e := exponent
		switch e[0] {
		case '+':
			e = e[1:]
		case '-':
			sign = -1
			e = e[1:]
		}
		if e == "" || len(e) > 9 {
			return Dec{}, ErrInvalid
		}
		for i := 0; i < len(e); i++ {
			if e[i] < '0' || e[i] > '9' {
				return Dec{}, ErrInvalid
			}
			exp = exp*10 + int(e[i]-'0')
		}
		exp *= sign
	}
	exp -= len(fracPart)
	c, ok := new(big.Int).SetString(strings.TrimLeft(digits, "0"), 10)
	if !ok {
		c = new(big.Int)
	}
	return mk(neg, c, exp), nil
}

func MustParse(text string) Dec {
	d, err := Parse(text)
	if err != nil {
		panic("pydec: invalid literal " + text)
	}
	return d
}

func (d Dec) IsZero() bool { return d.c == nil || d.c.Sign() == 0 }

func (d Dec) Sign() int {
	if d.IsZero() {
		return 0
	}
	if d.neg {
		return -1
	}
	return 1
}

func (d Dec) Exp() int { return d.exp }

func (d Dec) digitString() string {
	if d.IsZero() {
		return "0"
	}
	return d.c.String()
}

func (d Dec) String() string {
	sign := ""
	if d.neg {
		sign = "-"
	}
	digits := d.digitString()
	leftdigits := d.exp + len(digits)
	var dotplace int
	if d.exp <= 0 && leftdigits > -6 {
		dotplace = leftdigits
	} else {
		dotplace = 1
	}
	var intpart, fracpart string
	switch {
	case dotplace <= 0:
		intpart = "0"
		fracpart = "." + strings.Repeat("0", -dotplace) + digits
	case dotplace >= len(digits):
		intpart = digits + strings.Repeat("0", dotplace-len(digits))
	default:
		intpart = digits[:dotplace]
		fracpart = "." + digits[dotplace:]
	}
	exp := ""
	if leftdigits != dotplace {
		e := leftdigits - dotplace
		if e >= 0 {
			exp = "E+" + itoa(e)
		} else {
			exp = "E-" + itoa(-e)
		}
	}
	return sign + intpart + fracpart + exp
}

func (d Dec) FormatF() string {
	sign := ""
	if d.neg {
		sign = "-"
	}
	if d.IsZero() && d.exp > 0 {
		d = mk(d.neg, nil, 0)
	}
	digits := d.digitString()
	dotplace := d.exp + len(digits)
	var intpart, fracpart string
	switch {
	case dotplace < 0:
		intpart = "0"
		fracpart = strings.Repeat("0", -dotplace) + digits
	case dotplace > len(digits):
		intpart = digits + strings.Repeat("0", dotplace-len(digits))
	default:
		intpart = digits[:dotplace]
		if intpart == "" {
			intpart = "0"
		}
		fracpart = digits[dotplace:]
	}
	if fracpart != "" {
		return sign + intpart + "." + fracpart
	}
	return sign + intpart
}

func itoa(v int) string {
	return big.NewInt(int64(v)).String()
}

func fix(d Dec, prec int) Dec {
	if d.IsZero() {
		return mk(d.neg, nil, d.exp)
	}
	n := numDigits(d.c)
	expMin := n + d.exp - prec
	if d.exp >= expMin {
		return d
	}
	drop := expMin - d.exp
	q, r := new(big.Int).QuoRem(d.c, powTen(drop), new(big.Int))
	half := new(big.Int).Mul(bigFive, powTen(drop-1))
	switch r.Cmp(half) {
	case 1:
		q.Add(q, big.NewInt(1))
	case 0:
		if q.Bit(0) == 1 {
			q.Add(q, big.NewInt(1))
		}
	}
	if numDigits(q) > prec {
		q.Quo(q, bigTen)
		expMin++
	}
	return mk(d.neg, q, expMin)
}

func rescale(d Dec, exp int) Dec {
	if d.IsZero() {
		return mk(d.neg, nil, exp)
	}
	if d.exp >= exp {
		c := new(big.Int).Mul(d.c, powTen(d.exp-exp))
		return mk(d.neg, c, exp)
	}
	digits := numDigits(d.c) + d.exp - exp
	src := d
	if digits < 0 {
		src = mk(d.neg, big.NewInt(1), exp-1)
		digits = 0
	}
	n := numDigits(src.c)
	drop := n - digits
	q, r := new(big.Int).QuoRem(src.c, powTen(drop), new(big.Int))
	half := new(big.Int).Mul(bigFive, powTen(drop-1))
	switch r.Cmp(half) {
	case 1:
		q.Add(q, big.NewInt(1))
	case 0:
		if q.Bit(0) == 1 {
			q.Add(q, big.NewInt(1))
		}
	}
	return mk(d.neg, q, exp)
}

func Add(a, b Dec, prec int) Dec {
	exp := a.exp
	if b.exp < exp {
		exp = b.exp
	}
	if a.IsZero() && b.IsZero() {
		return fix(mk(a.neg && b.neg, nil, exp), prec)
	}
	if a.IsZero() {
		e := exp
		if b.exp-prec-1 > e {
			e = b.exp - prec - 1
		}
		return fix(rescale(b, e), prec)
	}
	if b.IsZero() {
		e := exp
		if a.exp-prec-1 > e {
			e = a.exp - prec - 1
		}
		return fix(rescale(a, e), prec)
	}

	type work struct {
		neg bool
		c   *big.Int
		exp int
	}
	op1 := &work{a.neg, new(big.Int).Set(a.c), a.exp}
	op2 := &work{b.neg, new(big.Int).Set(b.c), b.exp}
	tmp, other := op1, op2
	if op1.exp < op2.exp {
		tmp, other = op2, op1
	}
	tmpLen, otherLen := numDigits(tmp.c), numDigits(other.c)
	shiftExp := tmp.exp + minInt(-1, tmpLen-prec-2)
	if otherLen+other.exp-1 < shiftExp {
		other.c = big.NewInt(1)
		other.exp = shiftExp
	}
	tmp.c.Mul(tmp.c, powTen(tmp.exp-other.exp))
	tmp.exp = other.exp

	var resultNeg bool
	if op1.neg != op2.neg {
		if op1.c.Cmp(op2.c) == 0 {
			return fix(mk(false, nil, exp), prec)
		}
		if op1.c.Cmp(op2.c) < 0 {
			op1, op2 = op2, op1
		}
		if op1.neg {
			resultNeg = true
			op1.neg, op2.neg = op2.neg, op1.neg
		}
	} else if op1.neg {
		resultNeg = true
		op1.neg, op2.neg = false, false
	}
	c := new(big.Int)
	if !op2.neg {
		c.Add(op1.c, op2.c)
	} else {
		c.Sub(op1.c, op2.c)
	}
	return fix(mk(resultNeg, c, op1.exp), prec)
}

func Sub(a, b Dec, prec int) Dec {
	return Add(a, b.copyNegate(), prec)
}

func (d Dec) copyNegate() Dec { return Dec{neg: !d.neg, c: d.c, exp: d.exp} }

func Mul(a, b Dec, prec int) Dec {
	neg := a.neg != b.neg
	exp := a.exp + b.exp
	if a.IsZero() || b.IsZero() {
		return fix(mk(neg, nil, exp), prec)
	}
	return fix(mk(neg, new(big.Int).Mul(a.c, b.c), exp), prec)
}

func Div(a, b Dec, prec int) (Dec, error) {
	if b.IsZero() {
		return Dec{}, ErrDivisionByZero
	}
	neg := a.neg != b.neg
	if a.IsZero() {
		return fix(mk(neg, nil, a.exp-b.exp), prec), nil
	}
	shift := numDigits(b.c) - numDigits(a.c) + prec + 1
	exp := a.exp - b.exp - shift
	q, r := new(big.Int), new(big.Int)
	if shift >= 0 {
		q.QuoRem(new(big.Int).Mul(a.c, powTen(shift)), b.c, r)
	} else {
		q.QuoRem(a.c, new(big.Int).Mul(b.c, powTen(-shift)), r)
	}
	if r.Sign() != 0 {
		if new(big.Int).Mod(q, bigFive).Sign() == 0 {
			q.Add(q, big.NewInt(1))
		}
	} else {
		ideal := a.exp - b.exp
		m := new(big.Int)
		for exp < ideal {
			if m.Mod(q, bigTen).Sign() != 0 {
				break
			}
			q.Quo(q, bigTen)
			exp++
		}
	}
	return fix(mk(neg, q, exp), prec), nil
}

func MustDiv(a, b Dec, prec int) Dec {
	d, err := Div(a, b, prec)
	if err != nil {
		panic(err)
	}
	return d
}

func (d Dec) Abs(prec int) Dec {
	return fix(mk(false, d.c, d.exp), prec)
}

func (d Dec) Neg(prec int) Dec {
	if d.IsZero() {
		return fix(mk(false, nil, d.exp), prec)
	}
	return fix(mk(!d.neg, d.c, d.exp), prec)
}

func Cmp(a, b Dec) int {
	sa, sb := a.Sign(), b.Sign()
	if sa != sb {
		if sa < sb {
			return -1
		}
		return 1
	}
	if sa == 0 {
		return 0
	}
	ca, cb := a.c, b.c
	if a.exp > b.exp {
		ca = new(big.Int).Mul(ca, powTen(a.exp-b.exp))
	} else if b.exp > a.exp {
		cb = new(big.Int).Mul(cb, powTen(b.exp-a.exp))
	}
	c := ca.Cmp(cb)
	if sa < 0 {
		return -c
	}
	return c
}

func (d Dec) Cmp(o Dec) int { return Cmp(d, o) }

func (d Dec) Equal(o Dec) bool { return Cmp(d, o) == 0 }

func Max(a, b Dec) Dec {
	if Cmp(b, a) > 0 {
		return b
	}
	return a
}

func Min(a, b Dec) Dec {
	if Cmp(b, a) < 0 {
		return b
	}
	return a
}

func (d Dec) IsInteger() bool {
	if d.IsZero() || d.exp >= 0 {
		return true
	}
	return new(big.Int).Mod(d.c, powTen(-d.exp)).Sign() == 0
}

func (d Dec) Int64() (int64, bool) {
	if d.IsZero() {
		return 0, true
	}
	var v *big.Int
	if d.exp >= 0 {
		v = new(big.Int).Mul(d.c, powTen(d.exp))
	} else {
		v = new(big.Int).Quo(d.c, powTen(-d.exp))
	}
	if !v.IsInt64() {
		return 0, false
	}
	if d.neg {
		return -v.Int64(), true
	}
	return v.Int64(), true
}

func (d Dec) Float64() float64 {
	r := new(big.Rat)
	if d.IsZero() {
		return 0
	}
	if d.exp >= 0 {
		r.SetInt(new(big.Int).Mul(d.c, powTen(d.exp)))
	} else {
		r.SetFrac(d.c, powTen(-d.exp))
	}
	f, _ := r.Float64()
	if d.neg {
		return -f
	}
	return f
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
