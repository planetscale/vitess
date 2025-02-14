/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package charset

import (
	"strings"
	"unicode"
)

/*
*s++ = MY_FILENAME_ESCAPE;
if ((wc >= 0x00C0 && wc <= 0x05FF && (code = uni_0C00_05FF[wc - 0x00C0])) ||
		(wc >= 0x1E00 && wc <= 0x1FFF && (code = uni_1E00_1FFF[wc - 0x1E00])) ||
		(wc >= 0x2160 && wc <= 0x217F && (code = uni_2160_217F[wc - 0x2160])) ||
		(wc >= 0x24B0 && wc <= 0x24EF && (code = uni_24B0_24EF[wc - 0x24B0])) ||
		(wc >= 0xFF20 && wc <= 0xFF5F && (code = uni_FF20_FF5F[wc - 0xFF20]))) {
	*s++ = (code / 80) + 0x30;
	*s++ = (code % 80) + 0x30;
	return 3;
}
*/

// TablenameToFilename is a rewrite of MySQL's `tablename_to_filename` utility function.
// InnoDB table names are in the form of schema_name/table_name, except each of the tokens are encoded using this function.
// For simple characters there is no change, but special characters get encoded. Thus, the table `tbl-fts` in `test` db
// will be encoded as `test/tbl@002dfts`
//
// Original encoding function:
//
//	  https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/strings/ctype-utf8.cc#L6961-L6984
//
//				static int my_wc_mb_filename(const CHARSET_INFO *cs [[maybe_unused]],
//																		my_wc_t wc, uchar *s, uchar *e) {
//					int code;
//					char hex[] = "0123456789abcdef";
//
//					if (s >= e) return MY_CS_TOOSMALL;
//
//					if (wc < 128 && filename_safe_char[wc]) {
//						*s = (uchar)wc;
//						return 1;
//					}
//
//					if (s + 3 > e) return MY_CS_TOOSMALL3;
//
//					*s++ = MY_FILENAME_ESCAPE;
//					if ((wc >= 0x00C0 && wc <= 0x05FF && (code = uni_0C00_05FF[wc - 0x00C0])) ||
//							(wc >= 0x1E00 && wc <= 0x1FFF && (code = uni_1E00_1FFF[wc - 0x1E00])) ||
//							(wc >= 0x2160 && wc <= 0x217F && (code = uni_2160_217F[wc - 0x2160])) ||
//							(wc >= 0x24B0 && wc <= 0x24EF && (code = uni_24B0_24EF[wc - 0x24B0])) ||
//							(wc >= 0xFF20 && wc <= 0xFF5F && (code = uni_FF20_FF5F[wc - 0xFF20]))) {
//						*s++ = (code / 80) + 0x30;
//						*s++ = (code % 80) + 0x30;
//						return 3;
//					}
//
//					/* Non letter */
//					if (s + 5 > e) return MY_CS_TOOSMALL5;
//
//					*s++ = hex[(wc >> 12) & 15];
//					*s++ = hex[(wc >> 8) & 15];
//					*s++ = hex[(wc >> 4) & 15];
//					*s++ = hex[(wc)&15];
//					return 5;
//				}
//
// See also MySQL docs: https://dev.mysql.com/doc/refman/8.0/en/identifier-mapping.html
func TablenameToFilename(name string) string {
	var b strings.Builder
	for _, wc := range name {
		if wc < 128 && filename_safe_char[wc] == 1 {
			b.WriteRune(wc)
			continue
		}

		b.WriteRune('@')

		var code uint16
		switch {
		case wc >= 0x00C0 && wc <= 0x05FF:
			code = uni_0C00_05FF[wc-0x00C0]
		case wc >= 0x1E00 && wc <= 0x1FFF:
			code = uni_1E00_1FFF[wc-0x1E00]
		case wc >= 0x2160 && wc <= 0x217F:
			code = uni_2160_217F[wc-0x2160]
		case wc >= 0x24B0 && wc <= 0x24EF:
			code = uni_24B0_24EF[wc-0x24B0]
		case wc >= 0xFF20 && wc <= 0xFF5F:
			code = uni_FF20_FF5F[wc-0xFF20]
		}
		if code != 0 {
			// One of specifically addressed character sets
			b.WriteRune(unicode.ToLower(rune(code/80) + 0x30))
			b.WriteRune(unicode.ToLower(rune(code%80) + 0x30))
			continue
		}
		// Other characters
		b.WriteByte(hexSequence[(wc>>12)&15])
		b.WriteByte(hexSequence[(wc>>8)&15])
		b.WriteByte(hexSequence[(wc>>4)&15])
		b.WriteByte(hexSequence[(wc)&15])
	}
	return b.String()
}

var (
	hexSequence = "0123456789abcdef"

	filename_safe_char = []byte{
		1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* ................ */
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* ................ */
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /*  !"#$%&'()*+,-./ */
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, /* 0123456789:;<=>? */
		0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* @ABCDEFGHIJKLMNO */
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, /* PQRSTUVWXYZ[\]^_ */
		0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* `abcdefghijklmno */
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, /* pqrstuvwxyz{|}~. */
	}

	/* 00C0-05FF */
	uni_0C00_05FF = []uint16{
		0x0017, 0x0018, 0x0019, 0x001A, 0x001B, 0x001C, 0x001D, 0x001E, 0x001F,
		0x0020, 0x0021, 0x0022, 0x0023, 0x0024, 0x0025, 0x0026, 0x0027, 0x0028,
		0x0029, 0x002A, 0x0067, 0x0068, 0x0069, 0x0000, 0x006B, 0x006C, 0x006D,
		0x006E, 0x006F, 0x0070, 0x0071, 0x008A, 0x0037, 0x0038, 0x0039, 0x003A,
		0x003B, 0x003C, 0x003D, 0x003E, 0x003F, 0x0040, 0x0041, 0x0042, 0x0043,
		0x0044, 0x0045, 0x0046, 0x0047, 0x0048, 0x0049, 0x004A, 0x0087, 0x0088,
		0x0089, 0x0000, 0x008B, 0x008C, 0x008D, 0x008E, 0x008F, 0x0090, 0x0091,
		0x0092, 0x0073, 0x0093, 0x0074, 0x0094, 0x0075, 0x0095, 0x0076, 0x0096,
		0x0077, 0x0097, 0x0078, 0x0098, 0x0079, 0x0099, 0x007A, 0x009A, 0x00B7,
		0x00D7, 0x00B8, 0x00D8, 0x00B9, 0x00D9, 0x00BA, 0x00DA, 0x00BB, 0x00DB,
		0x00BC, 0x00DC, 0x00BD, 0x00DD, 0x00BE, 0x00DE, 0x00BF, 0x00DF, 0x00C0,
		0x00E0, 0x00C1, 0x00E1, 0x00C2, 0x00E2, 0x00C3, 0x00E3, 0x00C4, 0x00E4,
		0x00C5, 0x00E5, 0x00C6, 0x00E6, 0x0000, 0x00E7, 0x00C8, 0x00E8, 0x00C9,
		0x00E9, 0x00CA, 0x00EA, 0x0127, 0x0108, 0x0128, 0x0109, 0x0129, 0x010A,
		0x012A, 0x010B, 0x012B, 0x010C, 0x012C, 0x010D, 0x012D, 0x010E, 0x012E,
		0x010F, 0x012F, 0x0130, 0x0111, 0x0131, 0x0112, 0x0132, 0x0113, 0x0133,
		0x0114, 0x0134, 0x0115, 0x0135, 0x0116, 0x0136, 0x0117, 0x0137, 0x0118,
		0x0138, 0x0119, 0x0139, 0x011A, 0x013A, 0x0157, 0x0177, 0x0158, 0x0178,
		0x0159, 0x0179, 0x015A, 0x017A, 0x015B, 0x017B, 0x015C, 0x017C, 0x015D,
		0x017D, 0x015E, 0x017E, 0x015F, 0x017F, 0x0160, 0x0180, 0x0161, 0x0181,
		0x0162, 0x0182, 0x0163, 0x0183, 0x0072, 0x0164, 0x0184, 0x0165, 0x0185,
		0x0166, 0x0186, 0x0187, 0x1161, 0x0A86, 0x07B1, 0x11B1, 0x0801, 0x1201,
		0x0AD6, 0x0851, 0x1251, 0x0B76, 0x0BC6, 0x08A1, 0x12A1, 0x12F1, 0x0D52,
		0x0C66, 0x0D06, 0x0941, 0x1341, 0x0857, 0x0947, 0x1391, 0x0B27, 0x0AD7,
		0x09E1, 0x13E1, 0x1431, 0x1481, 0x0D07, 0x07B8, 0x14D1, 0x08A8, 0x0B21,
		0x1521, 0x0B71, 0x1571, 0x0BC1, 0x15C1, 0x0C18, 0x0C11, 0x1611, 0x0D08,
		0x1661, 0x16B1, 0x0D01, 0x1701, 0x0859, 0x0D51, 0x1751, 0x08F9, 0x0949,
		0x0762, 0x1162, 0x07B2, 0x11B2, 0x0B79, 0x0802, 0x1202, 0x1252, 0x12A2,
		0x0992, 0x1392, 0x1342, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x09E2,
		0x0000, 0x13E2, 0x0A32, 0x0000, 0x1432, 0x0A82, 0x0000, 0x1482, 0x0AD2,
		0x14D2, 0x0B22, 0x1522, 0x0B72, 0x1572, 0x0BC2, 0x15C2, 0x0C12, 0x1612,
		0x0C62, 0x1662, 0x0CB2, 0x16B2, 0x0D02, 0x1702, 0x1752, 0x0763, 0x1163,
		0x07B3, 0x11B3, 0x0803, 0x1203, 0x0853, 0x1253, 0x08A3, 0x12A3, 0x08F3,
		0x12F3, 0x0943, 0x1343, 0x0993, 0x1393, 0x09E3, 0x13E3, 0x1433, 0x0A83,
		0x0000, 0x1483, 0x0AD3, 0x14D3, 0x0991, 0x0000, 0x0B23, 0x1523, 0x0B73,
		0x1573, 0x0BC3, 0x15C3, 0x0C13, 0x1613, 0x0C63, 0x1663, 0x0CB3, 0x16B3,
		0x0D03, 0x1703, 0x0D53, 0x1753, 0x0764, 0x1164, 0x07B4, 0x11B4, 0x0804,
		0x1204, 0x0854, 0x1254, 0x08A4, 0x12A4, 0x08F4, 0x12F4, 0x0944, 0x1344,
		0x0994, 0x1394, 0x09E4, 0x13E4, 0x0A34, 0x1434, 0x0A84, 0x1484, 0x0AD4,
		0x14D4, 0x0AD1, 0x1524, 0x0B74, 0x1574, 0x0BC4, 0x15C4, 0x0C14, 0x1614,
		0x0C64, 0x1664, 0x0CB4, 0x16B4, 0x0D04, 0x1704, 0x0D54, 0x1754, 0x0765,
		0x1165, 0x07B5, 0x11B5, 0x1205, 0x1255, 0x12A5, 0x12F5, 0x1345, 0x1395,
		0x09E5, 0x0A35, 0x1435, 0x0A31, 0x0A85, 0x14D5, 0x1525, 0x0C19, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x1396, 0x13E6, 0x1436, 0x1486, 0x14D6,
		0x1526, 0x1576, 0x15C6, 0x1616, 0x1666, 0x16B6, 0x1706, 0x1756, 0x1167,
		0x11B7, 0x1207, 0x1257, 0x12A7, 0x12F7, 0x1347, 0x1397, 0x13E7, 0x1437,
		0x1487, 0x14D7, 0x1527, 0x1577, 0x15C7, 0x1617, 0x1667, 0x16B7, 0x1707,
		0x1757, 0x1168, 0x11B8, 0x1208, 0x1258, 0x12A8, 0x12F8, 0x1348, 0x1398,
		0x13E8, 0x1438, 0x1488, 0x14D8, 0x1528, 0x1578, 0x15C8, 0x1618, 0x1668,
		0x16B8, 0x1708, 0x1758, 0x1169, 0x11B9, 0x1209, 0x1259, 0x12A9, 0x12F9,
		0x1349, 0x1399, 0x13E9, 0x1439, 0x1489, 0x14D9, 0x1529, 0x1579, 0x15C9,
		0x1619, 0x1669, 0x16B9, 0x1709, 0x1759, 0x116A, 0x11BA, 0x120A, 0x125A,
		0x12AA, 0x12FA, 0x134A, 0x139A, 0x13EA, 0x143A, 0x148A, 0x14DA, 0x152A,
		0x157A, 0x15CA, 0x161A, 0x166A, 0x16BA, 0x170A, 0x175A, 0x116B, 0x11BB,
		0x120B, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x01F7,
		0x0000, 0x01F8, 0x01F9, 0x01FA, 0x0000, 0x0253, 0x0000, 0x0254, 0x0255,
		0x01D9, 0x01FC, 0x0257, 0x01FE, 0x01FF, 0x0200, 0x0201, 0x0202, 0x0258,
		0x0204, 0x02A7, 0x0206, 0x0207, 0x0208, 0x0209, 0x020A, 0x0299, 0x0248,
		0x0000, 0x02A9, 0x024B, 0x024C, 0x0298, 0x024E, 0x024F, 0x0250, 0x0251,
		0x0252, 0x0217, 0x0218, 0x0219, 0x021A, 0x021B, 0x021C, 0x021D, 0x021E,
		0x021F, 0x0220, 0x0221, 0x0222, 0x0223, 0x0224, 0x0225, 0x0226, 0x0227,
		0x0228, 0x0229, 0x022A, 0x0267, 0x0268, 0x0269, 0x026A, 0x026B, 0x026C,
		0x026D, 0x026E, 0x026F, 0x0270, 0x0271, 0x0272, 0x0273, 0x0274, 0x0275,
		0x0000, 0x0277, 0x0278, 0x0259, 0x025A, 0x0297, 0x02B8, 0x02B9, 0x02BA,
		0x0000, 0x02BB, 0x029C, 0x02BC, 0x029D, 0x02BD, 0x029E, 0x02BE, 0x029F,
		0x02BF, 0x02A0, 0x02C0, 0x02A1, 0x02C1, 0x02A2, 0x02C2, 0x02A3, 0x02C3,
		0x02A4, 0x02C4, 0x02A5, 0x02C5, 0x02A6, 0x02C6, 0x02C7, 0x02C8, 0x02C9,
		0x02CA, 0x0000, 0x0307, 0x0308, 0x0000, 0x0309, 0x0000, 0x0000, 0x030A,
		0x030B, 0x02EC, 0x02ED, 0x02EE, 0x0AF1, 0x0B41, 0x0B91, 0x0BE1, 0x0C31,
		0x0C81, 0x0CD1, 0x0D21, 0x0732, 0x0782, 0x07D2, 0x0822, 0x0872, 0x08C2,
		0x0912, 0x0962, 0x0730, 0x0780, 0x07D0, 0x0820, 0x0870, 0x08C0, 0x0910,
		0x0960, 0x09B0, 0x0A00, 0x0A50, 0x0AA0, 0x0AF0, 0x0B40, 0x0B90, 0x0BE0,
		0x0C30, 0x0C80, 0x0CD0, 0x0D20, 0x0731, 0x0781, 0x07D1, 0x0821, 0x0871,
		0x08C1, 0x0911, 0x0961, 0x09B1, 0x0A01, 0x0A51, 0x0AA1, 0x1130, 0x1180,
		0x11D0, 0x1220, 0x1270, 0x12C0, 0x1310, 0x1360, 0x13B0, 0x1400, 0x1450,
		0x14A0, 0x14F0, 0x1540, 0x1590, 0x15E0, 0x1630, 0x1680, 0x16D0, 0x1720,
		0x1131, 0x1181, 0x11D1, 0x1221, 0x1271, 0x12C1, 0x1311, 0x1361, 0x13B1,
		0x1401, 0x1451, 0x14A1, 0x14F1, 0x1541, 0x1591, 0x15E1, 0x1631, 0x1681,
		0x16D1, 0x1721, 0x1132, 0x1182, 0x11D2, 0x1222, 0x1272, 0x12C2, 0x1312,
		0x1362, 0x09B2, 0x13B2, 0x0A02, 0x1402, 0x0A52, 0x1452, 0x0AA2, 0x14A2,
		0x0AF2, 0x14F2, 0x0B42, 0x1542, 0x0B92, 0x1592, 0x0BE2, 0x15E2, 0x0C32,
		0x1632, 0x0C82, 0x1682, 0x0CD2, 0x16D2, 0x0D22, 0x1722, 0x0733, 0x1133,
		0x0783, 0x1183, 0x07D3, 0x11D3, 0x0823, 0x1223, 0x0873, 0x1273, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0913, 0x1313,
		0x0963, 0x1363, 0x09B3, 0x13B3, 0x0A03, 0x1403, 0x0A53, 0x1453, 0x0AA3,
		0x14A3, 0x0AF3, 0x14F3, 0x0B43, 0x1543, 0x0B93, 0x1593, 0x0BE3, 0x15E3,
		0x0C33, 0x1633, 0x0C83, 0x1683, 0x0CD3, 0x16D3, 0x0D23, 0x1723, 0x0734,
		0x1134, 0x0784, 0x1184, 0x07D4, 0x11D4, 0x0824, 0x1224, 0x0874, 0x1274,
		0x08C4, 0x12C4, 0x0914, 0x1314, 0x0964, 0x1364, 0x09B4, 0x13B4, 0x0A04,
		0x1404, 0x0A54, 0x1454, 0x0AA4, 0x14A4, 0x0AF4, 0x14F4, 0x0B44, 0x0B94,
		0x1594, 0x0BE4, 0x15E4, 0x0C34, 0x1634, 0x0C84, 0x1684, 0x0CD4, 0x16D4,
		0x0D24, 0x1724, 0x0735, 0x1135, 0x0000, 0x07D5, 0x11D5, 0x0825, 0x1225,
		0x0875, 0x1275, 0x08C5, 0x12C5, 0x0915, 0x1315, 0x0965, 0x1365, 0x09B5,
		0x13B5, 0x0A05, 0x1405, 0x0A55, 0x1455, 0x0AA5, 0x14A5, 0x0AF5, 0x14F5,
		0x0B45, 0x1545, 0x0B95, 0x1595, 0x0BE5, 0x15E5, 0x0C35, 0x1635, 0x0C85,
		0x1685, 0x0CD5, 0x16D5, 0x0D25, 0x1725, 0x0736, 0x1136, 0x0786, 0x1186,
		0x07D6, 0x11D6, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0A06,
		0x1406, 0x0A56, 0x1456, 0x0AA6, 0x14A6, 0x0AF6, 0x14F6, 0x0B46, 0x1546,
		0x0B96, 0x1596, 0x0BE6, 0x15E6, 0x0C36, 0x1636, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0787, 0x07D7, 0x0827, 0x0877, 0x08C7, 0x0917,
		0x0967, 0x09B7, 0x0A07, 0x0A57, 0x0AA7, 0x0AF7, 0x0B47, 0x0B97, 0x0BE7,
		0x0C37, 0x0C87, 0x0CD7, 0x0D27, 0x0738, 0x0788, 0x07D8, 0x0828, 0x0878,
		0x08C8, 0x0918, 0x0968, 0x09B8, 0x0A08, 0x0A58, 0x0AA8, 0x0AF8, 0x0B48,
		0x0B98, 0x0BE8, 0x0C38, 0x0C88, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x1187, 0x11D7, 0x1227,
		0x1277, 0x12C7, 0x1317, 0x1367, 0x13B7, 0x1407, 0x1457, 0x14A7, 0x14F7,
		0x1547, 0x1597, 0x15E7, 0x1637, 0x1687, 0x16D7, 0x1727, 0x1138, 0x1188,
		0x11D8, 0x1228, 0x1278, 0x12C8, 0x1318, 0x1368, 0x13B8, 0x1408, 0x1458,
		0x14A8, 0x14F8, 0x1548, 0x1598, 0x15E8, 0x1638, 0x1688, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000}

	/* 1E00-1FFF */
	uni_1E00_1FFF = []uint16{
		0x076C, 0x116C, 0x07BC, 0x11BC, 0x080C, 0x120C, 0x085C, 0x125C, 0x08AC,
		0x12AC, 0x08FC, 0x12FC, 0x094C, 0x134C, 0x099C, 0x139C, 0x09EC, 0x13EC,
		0x0A3C, 0x143C, 0x0A8C, 0x148C, 0x0ADC, 0x14DC, 0x0B2C, 0x152C, 0x0B7C,
		0x157C, 0x0BCC, 0x15CC, 0x0C1C, 0x161C, 0x0C6C, 0x166C, 0x0CBC, 0x16BC,
		0x0D0C, 0x170C, 0x0D5C, 0x175C, 0x076D, 0x116D, 0x07BD, 0x11BD, 0x080D,
		0x120D, 0x085D, 0x125D, 0x08AD, 0x12AD, 0x08FD, 0x12FD, 0x094D, 0x134D,
		0x099D, 0x139D, 0x09ED, 0x13ED, 0x0A3D, 0x143D, 0x0A8D, 0x148D, 0x0ADD,
		0x14DD, 0x0B2D, 0x152D, 0x0B7D, 0x157D, 0x0BCD, 0x15CD, 0x0C1D, 0x161D,
		0x0C6D, 0x166D, 0x0CBD, 0x16BD, 0x0D0D, 0x170D, 0x0D5D, 0x175D, 0x076E,
		0x116E, 0x07BE, 0x11BE, 0x080E, 0x120E, 0x085E, 0x125E, 0x08AE, 0x12AE,
		0x08FE, 0x12FE, 0x094E, 0x134E, 0x099E, 0x139E, 0x0770, 0x13EE, 0x0A3E,
		0x143E, 0x0A8E, 0x148E, 0x0ADE, 0x14DE, 0x0B2E, 0x152E, 0x0B7E, 0x157E,
		0x0BCE, 0x15CE, 0x0C1E, 0x161E, 0x0C6E, 0x166E, 0x0CBE, 0x16BE, 0x0D0E,
		0x170E, 0x0D5E, 0x175E, 0x076F, 0x116F, 0x07BF, 0x11BF, 0x080F, 0x120F,
		0x085F, 0x125F, 0x08AF, 0x12AF, 0x08FF, 0x12FF, 0x094F, 0x134F, 0x099F,
		0x139F, 0x09EF, 0x13EF, 0x0A3F, 0x143F, 0x0A8F, 0x148F, 0x0ADF, 0x14DF,
		0x0B2F, 0x152F, 0x0B7F, 0x157F, 0x0BCF, 0x15CF, 0x161F, 0x166F, 0x16BF,
		0x170F, 0x175F, 0x1170, 0x0000, 0x0000, 0x0000, 0x0000, 0x0900, 0x1300,
		0x0950, 0x1350, 0x09A0, 0x13A0, 0x09F0, 0x13F0, 0x0A40, 0x1440, 0x0A90,
		0x1490, 0x0AE0, 0x14E0, 0x0B30, 0x1530, 0x0B80, 0x1580, 0x0BD0, 0x15D0,
		0x0C20, 0x1620, 0x0C70, 0x1670, 0x0CC0, 0x16C0, 0x0D10, 0x1710, 0x0D60,
		0x1760, 0x0771, 0x1171, 0x07C1, 0x11C1, 0x0811, 0x1211, 0x0861, 0x1261,
		0x08B1, 0x12B1, 0x0901, 0x1301, 0x0951, 0x1351, 0x09A1, 0x13A1, 0x09F1,
		0x13F1, 0x0A41, 0x1441, 0x0A91, 0x1491, 0x0AE1, 0x14E1, 0x0B31, 0x1531,
		0x0B81, 0x1581, 0x0BD1, 0x15D1, 0x0C21, 0x1621, 0x0C71, 0x1671, 0x0CC1,
		0x16C1, 0x0D11, 0x1711, 0x0D61, 0x1761, 0x0772, 0x1172, 0x07C2, 0x11C2,
		0x0812, 0x1212, 0x0862, 0x1262, 0x08B2, 0x12B2, 0x0902, 0x1302, 0x0952,
		0x1352, 0x09A2, 0x13A2, 0x09F2, 0x13F2, 0x0A42, 0x1442, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x0000, 0x1173, 0x11C3, 0x1213, 0x1263, 0x12B3,
		0x1303, 0x1353, 0x13A3, 0x0773, 0x07C3, 0x0813, 0x0863, 0x08B3, 0x0903,
		0x0953, 0x09A3, 0x13F3, 0x1443, 0x1493, 0x14E3, 0x1533, 0x1583, 0x0000,
		0x0000, 0x09F3, 0x0A43, 0x0A93, 0x0AE3, 0x0B33, 0x0B83, 0x0000, 0x0000,
		0x1713, 0x1763, 0x1174, 0x11C4, 0x1214, 0x1264, 0x12B4, 0x1304, 0x0D13,
		0x0D63, 0x0774, 0x07C4, 0x0814, 0x0864, 0x08B4, 0x0904, 0x1354, 0x13A4,
		0x13F4, 0x1444, 0x1494, 0x14E4, 0x1534, 0x1584, 0x0954, 0x09A4, 0x09F4,
		0x0A44, 0x0A94, 0x0AE4, 0x0B34, 0x0B84, 0x15D4, 0x1624, 0x1674, 0x16C4,
		0x1714, 0x1764, 0x0000, 0x0000, 0x0BD4, 0x0C24, 0x0C74, 0x0CC4, 0x0D14,
		0x0D64, 0x0000, 0x0000, 0x12B5, 0x1305, 0x1355, 0x13A5, 0x13F5, 0x1445,
		0x1495, 0x14E5, 0x0000, 0x0905, 0x0000, 0x09A5, 0x0000, 0x0A45, 0x0000,
		0x0AE5, 0x1675, 0x16C5, 0x1715, 0x1765, 0x1176, 0x11C6, 0x1216, 0x1266,
		0x0C75, 0x0CC5, 0x0D15, 0x0D65, 0x0776, 0x07C6, 0x0816, 0x0866, 0x12B6,
		0x1306, 0x1356, 0x13A6, 0x13F6, 0x1446, 0x1496, 0x14E6, 0x1536, 0x1586,
		0x15D6, 0x1626, 0x1676, 0x16C6, 0x0000, 0x0000, 0x1177, 0x11C7, 0x1217,
		0x1267, 0x12B7, 0x1307, 0x1357, 0x13A7, 0x0777, 0x07C7, 0x0817, 0x0867,
		0x08B7, 0x0907, 0x0957, 0x09A7, 0x13F7, 0x1447, 0x1497, 0x14E7, 0x1537,
		0x1587, 0x15D7, 0x1627, 0x09F7, 0x0A47, 0x0A97, 0x0AE7, 0x0B37, 0x0B87,
		0x0BD7, 0x0C27, 0x1677, 0x16C7, 0x1717, 0x1767, 0x1178, 0x11C8, 0x1218,
		0x1268, 0x0C77, 0x0CC7, 0x0D17, 0x0D67, 0x0778, 0x07C8, 0x0818, 0x0868,
		0x12B8, 0x1308, 0x1358, 0x13A8, 0x13F8, 0x0000, 0x1498, 0x14E8, 0x08B8,
		0x0908, 0x08B6, 0x0906, 0x09A8, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x1538, 0x1588, 0x15D8, 0x0000, 0x1678, 0x16C8, 0x0956, 0x09A6, 0x09F6,
		0x0A46, 0x0B88, 0x0000, 0x0000, 0x0000, 0x1718, 0x1768, 0x1179, 0x11C9,
		0x0000, 0x0000, 0x12B9, 0x1309, 0x0D18, 0x0D68, 0x0A96, 0x0AE6, 0x0000,
		0x0000, 0x0000, 0x0000, 0x13A9, 0x13F9, 0x1449, 0x1499, 0x14E9, 0x1539,
		0x1589, 0x15D9, 0x09A9, 0x09F9, 0x0BD6, 0x0C26, 0x0B39, 0x0000, 0x0000,
		0x0000, 0x0000, 0x0000, 0x16C9, 0x1719, 0x0000, 0x0000, 0x11CA, 0x121A,
		0x0B36, 0x0B86, 0x0C76, 0x0CC6, 0x0D19, 0x0000, 0x0000, 0x0000}

	/* 2160-217F */
	uni_2160_217F = []uint16{
		0x0739, 0x0789, 0x07D9, 0x0829, 0x0879, 0x08C9, 0x0919, 0x0969,
		0x09B9, 0x0A09, 0x0A59, 0x0AA9, 0x0AF9, 0x0B49, 0x0B99, 0x0BE9,
		0x1139, 0x1189, 0x11D9, 0x1229, 0x1279, 0x12C9, 0x1319, 0x1369,
		0x13B9, 0x1409, 0x1459, 0x14A9, 0x14F9, 0x1549, 0x1599, 0x15E9}

	/* 24B0-24EF */
	uni_24B0_24EF = []uint16{
		0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0511, 0x0512,
		0x0513, 0x0514, 0x0515, 0x0516, 0x0517, 0x0518, 0x0519, 0x051A,
		0x051B, 0x051C, 0x051D, 0x051E, 0x051F, 0x0520, 0x0521, 0x0522,
		0x0523, 0x0524, 0x0525, 0x0526, 0x0527, 0x0528, 0x0529, 0x052A,
		0x0531, 0x0532, 0x0533, 0x0534, 0x0535, 0x0536, 0x0537, 0x0538,
		0x0539, 0x053A, 0x053B, 0x053C, 0x053D, 0x053E, 0x053F, 0x0540,
		0x0541, 0x0542, 0x0543, 0x0544, 0x0545, 0x0546, 0x0547, 0x0548,
		0x0549, 0x054A, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000}

	/* FF20-FF5F */
	uni_FF20_FF5F = []uint16{
		0x0000, 0x0560, 0x05B0, 0x0600, 0x0650, 0x06A0, 0x06F0, 0x0740,
		0x0790, 0x07E0, 0x0830, 0x0880, 0x08D0, 0x0920, 0x0970, 0x09C0,
		0x0A10, 0x0A60, 0x0AB0, 0x0B00, 0x0B50, 0x0BA0, 0x0BF0, 0x0C40,
		0x0C90, 0x0CE0, 0x0D30, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000,
		0x0000, 0x0F60, 0x0FB0, 0x1000, 0x1050, 0x10A0, 0x10F0, 0x1140,
		0x1190, 0x11E0, 0x1230, 0x1280, 0x12D0, 0x1320, 0x1370, 0x13C0,
		0x1410, 0x1460, 0x14B0, 0x1500, 0x1550, 0x15A0, 0x15F0, 0x1640,
		0x1690, 0x16E0, 0x1730, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000}
)
