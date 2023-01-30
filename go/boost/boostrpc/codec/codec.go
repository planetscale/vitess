package codec

import (
	"errors"

	"github.com/gogo/protobuf/proto"
)

func Marshal(msg interface{}) ([]byte, error) {
	return msg.(proto.Marshaler).Marshal()
}

func Unmarshal(buf []byte, msg interface{}) error {
	return msg.(proto.Unmarshaler).Unmarshal(buf)
}

func JSONMarshal(msg interface{}) ([]byte, error) {
	return nil, errors.New("unimplemented")
}

func JSONUnmarshal(buf []byte, msg interface{}) error {
	return errors.New("unimplemented")
}
