// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TODO: remove hard-coded versions when we have implemented fractional weights.
// The current implementation is incompatible with later CLDR versions.
//go:generate go run maketables.go -cldr=23 -unicode=6.2.0

// Package collate contains types for comparing and sorting Unicode strings
// according to a given collation order.
package collate // import "vitess.io/vitess/go/mysql/collations/vindex/collate"

import (
	"vitess.io/vitess/go/mysql/collations/vindex/internal/colltab"
)

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator struct {
	iter colltab.Iter
}

const VIndexLocale = 21

// New returns a new Collator initialized for the given locale.
func New() *Collator {
	c := &Collator{}
	c.iter.Weighter = getTable(locales[VIndexLocale])
	return c
}

// Key returns the collation key for str.
// Passing the buffer buf may avoid memory allocations.
// The returned slice will point to an allocation in Buffer and will remain
// valid until the next call to buf.Reset().
func (c *Collator) Key(buf []byte, str []byte) []byte {
	// See https://www.unicode.org/reports/tr10/#Main_Algorithm for more details.
	return c.key(buf, c.getColElems(str))
}

func (c *Collator) key(buf []byte, w []colltab.Elem) []byte {
	return c.keyFromElems(buf, w)
}

func (c *Collator) getColElems(str []byte) []colltab.Elem {
	c.iter.SetInput(str)
	for c.iter.Next() {
	}
	return c.iter.Elems
}

func appendPrimary(key []byte, p int) []byte {
	// Convert to variable length encoding; supports up to 23 bits.
	if p <= 0x7FFF {
		key = append(key, uint8(p>>8), uint8(p))
	} else {
		key = append(key, uint8(p>>16)|0x80, uint8(p>>8), uint8(p))
	}
	return key
}

// keyFromElems converts the weights ws to a compact sequence of bytes.
// The result will be appended to the byte buffer in buf.
func (c *Collator) keyFromElems(buf []byte, ws []colltab.Elem) []byte {
	for _, v := range ws {
		if w := v.Primary(); w > 0 {
			buf = appendPrimary(buf, w)
		}
	}
	return buf
}
