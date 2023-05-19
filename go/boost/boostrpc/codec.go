package boostrpc

import (
	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/proto"
)

type ProtoCodec struct{}

func (ProtoCodec) Marshal(v any) ([]byte, error) {
	switch v := v.(type) {
	case gogoproto.Marshaler:
		return v.Marshal()
	case proto.Message:
		return proto.Marshal(v)
	default:
		panic("unsupported ProtoBuf message")
	}
}

func (ProtoCodec) Unmarshal(data []byte, v any) error {
	switch v := v.(type) {
	case gogoproto.Unmarshaler:
		return v.Unmarshal(data)
	case proto.Message:
		return proto.Unmarshal(data, v)
	default:
		panic("unsupported ProtoBuf message")
	}
}

func (ProtoCodec) Name() string {
	return "proto"
}
