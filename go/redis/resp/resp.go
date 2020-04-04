package resp

import (
	"bytes"
	"strconv"
)

var (
	// https://redis.io/topics/protocol#simple-string-reply
	SimpleOK = []byte("+OK\r\n")

	BulkEmpty = []byte("$0\r\n\r\n")

	// https://redis.io/topics/protocol#nil-reply
	BulkNull = []byte("$-1\r\n")

	simplePrefix = []byte("+")
	bulkPrefix   = []byte("$")
	crlf         = []byte("\r\n")
)

func SimpleString(buf []byte) []byte {
	// Format: "+OK\r\n"
	return bytes.Join([][]byte{
		simplePrefix,
		buf,
		crlf,
	}, nil)
}

// https://redis.io/topics/protocol#bulk-string-reply
func BulkString(buf []byte) []byte {
	// Format: "$6\r\nfoobar\r\n"
	return bytes.Join([][]byte{
		bulkPrefix,
		[]byte(strconv.Itoa(len(buf))),
		crlf,
		buf,
		crlf,
	}, nil)
}
