package uca

import "testing"

func TestByteShuffles(t *testing.T) {
	var input = [8]byte{
		'a', 'b', 'c', 'd',
		'e', 'f', 'g', 'h',
	}

	var output [8]uint16

	m := ucaFastWeight(&output, &input[0], &fastweightTable_uca900_page000L0)
	t.Logf("%#v, %08x", output, m)
}

// 10001010101010101010100000001000
// 10001010101010101010100010001000
