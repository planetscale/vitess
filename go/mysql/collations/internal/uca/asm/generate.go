package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

var ShuffleMask = []byte{
	0, 0x80, 0x80, 0x80,
	1, 0x80, 0x80, 0x80,
	2, 0x80, 0x80, 0x80,
	3, 0x80, 0x80, 0x80,
	4, 0x80, 0x80, 0x80,
	5, 0x80, 0x80, 0x80,
	6, 0x80, 0x80, 0x80,
	7, 0x80, 0x80, 0x80,

	0, 1, 4, 5,
	8, 9, 12, 13,
	0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80,
	16, 17, 20, 21,
	24, 25, 28, 29,
	0x80, 0x80, 0x80, 0x80,
	0x80, 0x80, 0x80, 0x80,
}

func main() {
	data := GLOBL("u32ShuffleMask", RODATA|NOPTR)
	for i, b := range ShuffleMask {
		DATA(i, U8(b))
	}

	TEXT("ucaFastWeight", NOSPLIT, "func(dst *[8]uint16, p *byte, table *[256]uint32) uint32")

	p := Load(Param("p"), GP64())

	f := GP64()
	MOVQ(Imm(0x8080808080808080), f)
	TESTQ(f, Mem{Base: p})
	JNE(LabelRef("bail"))

	dst := Load(Param("dst"), GP64())
	tbl := Load(Param("table"), GP64())

	ymask := YMM()
	VPCMPEQD(ymask, ymask, ymask)

	y0 := YMM()
	VPBROADCASTQ(Mem{Base: p}, y0)
	VPSHUFB(data.Offset(0), y0, y0)

	y1 := YMM()
	VPGATHERDD(ymask, Mem{Base: tbl, Index: y0, Scale: 4}, y1)

	ret := GP32()
	VPMOVMSKB(y1, ret)
	ANDL(Imm(0x88888888), ret)
	Store(ret, ReturnIndex(0))

	VPSHUFB(data.Offset(32), y1, y1)
	MOVQ(y1.AsX(), Mem{Base: dst})
	VEXTRACTI128(Imm(0x01), y1, y1.AsX())
	MOVQ(y1.AsX(), Mem{Base: dst, Disp: 8})

	RET()

	Label("bail")
	XORL(ret, ret)
	RET()
	Generate()
}
