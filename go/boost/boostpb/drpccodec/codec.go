package drpccodec

import (
	"github.com/gogo/protobuf/proto"
)

func Marshal(msg interface{}) ([]byte, error) {
	return msg.(proto.Marshaler).Marshal()
}

func Unmarshal(buf []byte, msg interface{}) error {
	return msg.(proto.Unmarshaler).Unmarshal(buf)
}

func JSONMarshal(msg interface{}) ([]byte, error) {
	panic("unimplemented")
}

func JSONUnmarshal(buf []byte, msg interface{}) error {
	panic("unimplemented")
}
