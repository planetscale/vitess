// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Unicode table generator.
// Data read from the web.

package main

import (
	"flag"
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/regexp2/tools/genunicode/internal/gen"
	"vitess.io/vitess/go/mysql/regexp2/tools/genunicode/internal/ucd"
)

func main() {
	flag.Parse()
	setupOutput()
	loadChars() // always needed
	printCharNames()
	flushOutput()
}

var output *gen.CodeWriter

func setupOutput() {
	output = gen.NewCodeWriter()
}

func flushOutput() {
	output.WriteGoFile("tables.go", "unicode2")
}

func printf(format string, args ...interface{}) {
	fmt.Fprintf(output, format, args...)
}

func print(args ...interface{}) {
	fmt.Fprint(output, args...)
}

func println(args ...interface{}) {
	fmt.Fprintln(output, args...)
}

var category = map[string]bool{
	// Nd Lu etc.
	// We use one-character names to identify merged categories
	"L": true, // Lu Ll Lt Lm Lo
	"P": true, // Pc Pd Ps Pe Pu Pf Po
	"M": true, // Mn Mc Me
	"N": true, // Nd Nl No
	"S": true, // Sm Sc Sk So
	"Z": true, // Zs Zl Zp
	"C": true, // Cc Cf Cs Co Cn
}

// This contains only the properties we're interested in.
type Char struct {
	name      string
	codePoint rune // if zero, this index is not a valid code point.
	category  string
	upperCase rune
	lowerCase rune
	titleCase rune
	foldCase  rune // simple case folding
	caseOrbit rune // next in simple case folding orbit
}

const MaxChar = 0x10FFFF

var chars = make([]Char, MaxChar+1)

func loadChars() {
	ucd.Parse(gen.OpenUCDFile("UnicodeData.txt"), func(p *ucd.Parser) {
		c := Char{codePoint: p.Rune(0)}
		c.name = p.String(1)
		if strings.HasPrefix(c.name, "<") {
			c.name = ""
		}

		getRune := func(field int) rune {
			if p.String(field) == "" {
				return 0
			}
			return p.Rune(field)
		}

		c.category = p.String(ucd.GeneralCategory)
		category[c.category] = true
		switch c.category {
		case "Nd":
			// Decimal digit
			p.Int(ucd.NumericValue)
		case "Lu":
			c.upperCase = getRune(ucd.CodePoint)
			c.lowerCase = getRune(ucd.SimpleLowercaseMapping)
			c.titleCase = getRune(ucd.SimpleTitlecaseMapping)
		case "Ll":
			c.upperCase = getRune(ucd.SimpleUppercaseMapping)
			c.lowerCase = getRune(ucd.CodePoint)
			c.titleCase = getRune(ucd.SimpleTitlecaseMapping)
		case "Lt":
			c.upperCase = getRune(ucd.SimpleUppercaseMapping)
			c.lowerCase = getRune(ucd.SimpleLowercaseMapping)
			c.titleCase = getRune(ucd.CodePoint)
		default:
			c.upperCase = getRune(ucd.SimpleUppercaseMapping)
			c.lowerCase = getRune(ucd.SimpleLowercaseMapping)
			c.titleCase = getRune(ucd.SimpleTitlecaseMapping)
		}

		chars[c.codePoint] = c
	})
}

func printCharNames() {
	println("var CharacterNames = map[string]rune{")
	for _, c := range chars {
		if c.name == "" {
			continue
		}
		printf("%q: 0x%x,\n", c.name, c.codePoint)
	}
	println("}")
}
