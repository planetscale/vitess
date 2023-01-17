package common

import (
	"encoding/binary"
	"unsafe"

	"vitess.io/vitess/go/hack"
)

type Columns string

func ColumnsFrom(cols []int) Columns {
	var buf []byte
	for _, col := range cols {
		buf = binary.AppendUvarint(buf, uint64(col))
	}
	return *(*Columns)(unsafe.Pointer(&buf))
}

func (c Columns) ForEach(yield func(int)) {
	buf := hack.StringBytes(string(c))
	for {
		val, n := binary.Uvarint(buf)
		if n <= 0 {
			if n < 0 {
				panic("invalid encoding for Columns")
			}
			return
		}
		yield(int(val))
		buf = buf[n:]
	}
}

func (c Columns) Contains(target int) bool {
	buf := hack.StringBytes(string(c))
	for {
		val, n := binary.Uvarint(buf)
		if n <= 0 {
			return false
		}
		if int(val) == target {
			return true
		}
		buf = buf[n:]
	}
}

func (c Columns) ToSlice() (out []int) {
	c.ForEach(func(i int) {
		out = append(out, i)
	})
	return
}
