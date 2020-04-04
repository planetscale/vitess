package resp_test

import (
	"testing"

	"vitess.io/vitess/go/redis/resp"
	"vitess.io/vitess/go/test/utils"
)

func TestSimpleString(t *testing.T) {
	tcs := []struct {
		in  string
		out string
	}{
		{
			in:  "",
			out: "+\r\n",
		},
		{
			in:  "foobar",
			out: "+foobar\r\n",
		},
	}

	for _, tc := range tcs {
		wantOut := []byte(tc.out)
		out := resp.SimpleString([]byte(tc.in))
		utils.MustMatch(t, wantOut, out, "out doesn't match")
	}
}

func TestBulkString(t *testing.T) {
	tcs := []struct {
		in  string
		out string
	}{
		{
			in:  "",
			out: "$0\r\n\r\n",
		},
		{
			in:  "foobar",
			out: "$6\r\nfoobar\r\n",
		},
	}

	for _, tc := range tcs {
		wantOut := []byte(tc.out)
		out := resp.BulkString([]byte(tc.in))
		utils.MustMatch(t, wantOut, out, "out doesn't match")
	}
}
