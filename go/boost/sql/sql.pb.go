// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: sql.proto

package sql

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
	vitess_io_vitess_go_mysql_collations "vitess.io/vitess/go/mysql/collations"
	vitess_io_vitess_go_sqltypes "vitess.io/vitess/go/sqltypes"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Record struct {
	Row      Row   `protobuf:"bytes,1,opt,name=row,proto3,casttype=Row" json:"row,omitempty"`
	Offset   int32 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	Positive bool  `protobuf:"varint,3,opt,name=positive,proto3" json:"positive,omitempty"`
}

func (m *Record) Reset()         { *m = Record{} }
func (m *Record) String() string { return proto.CompactTextString(m) }
func (*Record) ProtoMessage()    {}
func (*Record) Descriptor() ([]byte, []int) {
	return fileDescriptor_698b595ef5cec090, []int{0}
}
func (m *Record) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Record) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Record.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Record) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Record.Merge(m, src)
}
func (m *Record) XXX_Size() int {
	return m.Size()
}
func (m *Record) XXX_DiscardUnknown() {
	xxx_messageInfo_Record.DiscardUnknown(m)
}

var xxx_messageInfo_Record proto.InternalMessageInfo

type Type struct {
	T         vitess_io_vitess_go_sqltypes.Type       `protobuf:"varint,1,opt,name=t,proto3,casttype=vitess.io/vitess/go/sqltypes.Type" json:"t,omitempty"`
	Collation vitess_io_vitess_go_mysql_collations.ID `protobuf:"varint,2,opt,name=collation,proto3,casttype=vitess.io/vitess/go/mysql/collations.ID" json:"collation,omitempty"`
	Length    int32                                   `protobuf:"varint,3,opt,name=length,proto3" json:"length,omitempty"`
	Precision int32                                   `protobuf:"varint,4,opt,name=precision,proto3" json:"precision,omitempty"`
	Nullable  bool                                    `protobuf:"varint,5,opt,name=nullable,proto3" json:"nullable,omitempty"`
}

func (m *Type) Reset()         { *m = Type{} }
func (m *Type) String() string { return proto.CompactTextString(m) }
func (*Type) ProtoMessage()    {}
func (*Type) Descriptor() ([]byte, []int) {
	return fileDescriptor_698b595ef5cec090, []int{1}
}
func (m *Type) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Type) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Type.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Type) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Type.Merge(m, src)
}
func (m *Type) XXX_Size() int {
	return m.Size()
}
func (m *Type) XXX_DiscardUnknown() {
	xxx_messageInfo_Type.DiscardUnknown(m)
}

var xxx_messageInfo_Type proto.InternalMessageInfo

type Expr struct {
	Expr string `protobuf:"bytes,1,opt,name=expr,proto3" json:"expr,omitempty"`
}

func (m *Expr) Reset()         { *m = Expr{} }
func (m *Expr) String() string { return proto.CompactTextString(m) }
func (*Expr) ProtoMessage()    {}
func (*Expr) Descriptor() ([]byte, []int) {
	return fileDescriptor_698b595ef5cec090, []int{2}
}
func (m *Expr) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Expr) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Expr.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Expr) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Expr.Merge(m, src)
}
func (m *Expr) XXX_Size() int {
	return m.Size()
}
func (m *Expr) XXX_DiscardUnknown() {
	xxx_messageInfo_Expr.DiscardUnknown(m)
}

var xxx_messageInfo_Expr proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Record)(nil), "sql.Record")
	proto.RegisterType((*Type)(nil), "sql.Type")
	proto.RegisterType((*Expr)(nil), "sql.Expr")
}

func init() { proto.RegisterFile("sql.proto", fileDescriptor_698b595ef5cec090) }

var fileDescriptor_698b595ef5cec090 = []byte{
	// 327 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x91, 0x3f, 0x6f, 0xf2, 0x30,
	0x10, 0xc6, 0xe3, 0x37, 0x09, 0x2f, 0xb1, 0xd4, 0xc5, 0xaa, 0xaa, 0x14, 0xb5, 0x86, 0x22, 0x55,
	0x45, 0xaa, 0x94, 0x0c, 0x8c, 0xdd, 0xa2, 0x76, 0x60, 0xb5, 0x2a, 0x55, 0xea, 0x06, 0xd4, 0xa4,
	0x91, 0x5c, 0xce, 0xb1, 0x5d, 0xfe, 0x7c, 0x8b, 0x7e, 0x2c, 0x46, 0x46, 0x26, 0xd4, 0xc2, 0xb7,
	0x60, 0xaa, 0xec, 0x50, 0x58, 0xd8, 0x9e, 0xdf, 0x5d, 0x9e, 0xdc, 0x73, 0x67, 0x1c, 0xe9, 0x52,
	0x24, 0x52, 0x81, 0x01, 0xe2, 0xeb, 0x52, 0x34, 0xce, 0x73, 0xc8, 0xc1, 0x71, 0x6a, 0x55, 0xd5,
	0x6a, 0xbf, 0xe0, 0x1a, 0xe3, 0x43, 0x50, 0x6f, 0xe4, 0x12, 0xfb, 0x0a, 0xa6, 0x31, 0x6a, 0xa1,
	0x4e, 0x94, 0xfd, 0xdf, 0xad, 0x9b, 0x3e, 0x83, 0x29, 0xb3, 0x35, 0x72, 0x81, 0x6b, 0x30, 0x1a,
	0x69, 0x6e, 0xe2, 0x7f, 0x2d, 0xd4, 0x09, 0xd9, 0x9e, 0x48, 0x03, 0xd7, 0x25, 0xe8, 0xc2, 0x14,
	0x13, 0x1e, 0xfb, 0x2d, 0xd4, 0xa9, 0xb3, 0x03, 0xb7, 0x57, 0x08, 0x07, 0xcf, 0x73, 0xc9, 0x49,
	0x17, 0x23, 0xe3, 0xfe, 0x1a, 0x66, 0xb7, 0xbb, 0x75, 0xf3, 0x66, 0x52, 0x18, 0xae, 0x75, 0x52,
	0x40, 0x5a, 0xa9, 0x34, 0x87, 0x54, 0x97, 0xc2, 0xcc, 0x25, 0xd7, 0x89, 0x75, 0x30, 0x64, 0x48,
	0x0f, 0x47, 0x43, 0x10, 0xa2, 0x6f, 0x0a, 0x18, 0xbb, 0xa1, 0x67, 0xd9, 0xfd, 0x6e, 0xdd, 0xbc,
	0x3b, 0x65, 0xfe, 0x98, 0xeb, 0x52, 0xa4, 0x87, 0xcf, 0x75, 0xd2, 0x7b, 0x64, 0x47, 0xb7, 0x0d,
	0x2f, 0xf8, 0x38, 0x37, 0xef, 0x2e, 0x62, 0xc8, 0xf6, 0x44, 0xae, 0x70, 0x24, 0x15, 0x1f, 0x16,
	0xda, 0x8e, 0x08, 0x5c, 0xeb, 0x58, 0xb0, 0xab, 0x8d, 0x3f, 0x85, 0xe8, 0x0f, 0x04, 0x8f, 0xc3,
	0x6a, 0xb5, 0x3f, 0x6e, 0x37, 0x70, 0xf0, 0x34, 0x93, 0x8a, 0x10, 0x1c, 0xf0, 0x99, 0x54, 0xd5,
	0xc9, 0x98, 0xd3, 0xd9, 0xc3, 0xe2, 0x87, 0x7a, 0x8b, 0x0d, 0x45, 0xcb, 0x0d, 0x45, 0xdf, 0x1b,
	0x8a, 0xbe, 0xb6, 0xd4, 0x5b, 0x6e, 0xa9, 0xb7, 0xda, 0x52, 0xef, 0xf5, 0xfa, 0x54, 0xfe, 0x01,
	0x80, 0x36, 0xf6, 0x04, 0x83, 0x9a, 0x7b, 0x93, 0xee, 0x6f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x96,
	0xb4, 0x4a, 0x53, 0xbb, 0x01, 0x00, 0x00,
}

func (m *Record) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Record) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Record) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Positive {
		i--
		if m.Positive {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x18
	}
	if m.Offset != 0 {
		i = encodeVarintSql(dAtA, i, uint64(m.Offset))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Row) > 0 {
		i -= len(m.Row)
		copy(dAtA[i:], m.Row)
		i = encodeVarintSql(dAtA, i, uint64(len(m.Row)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Type) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Type) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Type) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Nullable {
		i--
		if m.Nullable {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x28
	}
	if m.Precision != 0 {
		i = encodeVarintSql(dAtA, i, uint64(m.Precision))
		i--
		dAtA[i] = 0x20
	}
	if m.Length != 0 {
		i = encodeVarintSql(dAtA, i, uint64(m.Length))
		i--
		dAtA[i] = 0x18
	}
	if m.Collation != 0 {
		i = encodeVarintSql(dAtA, i, uint64(m.Collation))
		i--
		dAtA[i] = 0x10
	}
	if m.T != 0 {
		i = encodeVarintSql(dAtA, i, uint64(m.T))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *Expr) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Expr) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Expr) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Expr) > 0 {
		i -= len(m.Expr)
		copy(dAtA[i:], m.Expr)
		i = encodeVarintSql(dAtA, i, uint64(len(m.Expr)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintSql(dAtA []byte, offset int, v uint64) int {
	offset -= sovSql(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Record) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Row)
	if l > 0 {
		n += 1 + l + sovSql(uint64(l))
	}
	if m.Offset != 0 {
		n += 1 + sovSql(uint64(m.Offset))
	}
	if m.Positive {
		n += 2
	}
	return n
}

func (m *Type) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.T != 0 {
		n += 1 + sovSql(uint64(m.T))
	}
	if m.Collation != 0 {
		n += 1 + sovSql(uint64(m.Collation))
	}
	if m.Length != 0 {
		n += 1 + sovSql(uint64(m.Length))
	}
	if m.Precision != 0 {
		n += 1 + sovSql(uint64(m.Precision))
	}
	if m.Nullable {
		n += 2
	}
	return n
}

func (m *Expr) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Expr)
	if l > 0 {
		n += 1 + l + sovSql(uint64(l))
	}
	return n
}

func sovSql(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozSql(x uint64) (n int) {
	return sovSql(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Record) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowSql
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Record: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Record: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Row", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthSql
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthSql
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Row = Row(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
			}
			m.Offset = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Offset |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Positive", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Positive = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipSql(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthSql
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Type) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowSql
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Type: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Type: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field T", wireType)
			}
			m.T = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.T |= vitess_io_vitess_go_sqltypes.Type(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Collation", wireType)
			}
			m.Collation = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Collation |= vitess_io_vitess_go_mysql_collations.ID(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Length", wireType)
			}
			m.Length = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Length |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Precision", wireType)
			}
			m.Precision = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Precision |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Nullable", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Nullable = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipSql(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthSql
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Expr) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowSql
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Expr: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Expr: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Expr", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowSql
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthSql
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthSql
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Expr = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipSql(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthSql
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipSql(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowSql
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowSql
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowSql
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthSql
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupSql
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthSql
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthSql        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowSql          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupSql = fmt.Errorf("proto: unexpected end of group")
)