package decimal

// source: https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
// generated empirically with a manual loop:
//
//	for i := 1; i <= 65; i++ {
//		dec, err := NewFromMySQL(bytes.Repeat([]byte("9"), i))
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		byteLengths = append(byteLengths, len(dec.value.Bytes()))
//	}
var weightStringLengths = []int{
	0, 1, 1, 2, 2, 3, 3, 3, 4, 4, 5, 5, 5, 6, 6, 7, 7, 8, 8, 8,
	9, 9, 10, 10, 10, 11, 11, 12, 12, 13, 13, 13, 14, 14, 15, 15, 15,
	16, 16, 17, 17, 18, 18, 18, 19, 19, 20, 20, 20, 21, 21, 22, 22,
	23, 23, 23, 24, 24, 25, 25, 25, 26, 26, 27, 27, 27,
}

func (d Decimal) WeightString(dst []byte, length, precision int32) []byte {
	dec := d.rescale(-precision)
	dec = dec.Clamp(length-precision, precision)

	buf := make([]byte, weightStringLengths[length]+1)
	dec.value.FillBytes(buf[:])

	if dec.value.Sign() < 0 {
		for i := range buf {
			buf[i] ^= 0xff
		}
	}
	buf[0] ^= 0x80

	dst = append(dst, buf[:]...)
	return dst
}
