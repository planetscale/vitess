package litter

import (
	"io"
	"math"
	"strconv"
)

func printBool(w io.Writer, value bool) {
	if value {
		_, _ = w.Write([]byte("true"))
		return
	}
	_, _ = w.Write([]byte("false"))
}

func printInt(w io.Writer, val int64, base int) {
	_, _ = w.Write(strconv.AppendInt(nil, val, base))
}

func printUint(w io.Writer, val uint64, base int) {
	_, _ = w.Write(strconv.AppendUint(nil, val, base))
}

func printFloat(w io.Writer, val float64, precision int) {
	if math.Trunc(val) == val {
		// Ensure that floats like 1.0 are always printed with a decimal point
		_, _ = w.Write(strconv.AppendFloat(nil, val, 'f', 1, precision))
	} else {
		_, _ = w.Write(strconv.AppendFloat(nil, val, 'g', -1, precision))
	}
}

func printComplex(w io.Writer, c complex128, floatPrecision int) {
	_, _ = w.Write([]byte("complex"))
	printInt(w, int64(floatPrecision*2), 10)
	r := real(c)
	_, _ = w.Write([]byte("("))
	_, _ = w.Write(strconv.AppendFloat(nil, r, 'g', -1, floatPrecision))
	i := imag(c)
	if i >= 0 {
		_, _ = w.Write([]byte("+"))
	}
	_, _ = w.Write(strconv.AppendFloat(nil, i, 'g', -1, floatPrecision))
	_, _ = w.Write([]byte("i)"))
}

func printNil(w io.Writer) {
	_, _ = w.Write([]byte("nil"))
}
