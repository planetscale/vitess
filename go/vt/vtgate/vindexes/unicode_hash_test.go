package vindexes

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/testutil"
	"vitess.io/vitess/go/sqltypes"
)

type GoldenUnicodeHash struct {
	Input  []byte
	XXHash []byte
	MD5    []byte
}

func GenerateGoldenCases(t *testing.T, inputs [][]byte, outfile string) {
	var goldenHash []GoldenUnicodeHash

	for _, input := range inputs {
		v := sqltypes.NewVarChar(string(input))

		hash1, err := unicodeHash(vXXHash, v)
		require.NoError(t, err)

		hash2, err := unicodeHash(vMD5Hash, v)
		require.NoError(t, err)

		goldenHash = append(goldenHash, GoldenUnicodeHash{
			Input:  input,
			XXHash: hash1,
			MD5:    hash2,
		})
	}

	f, err := os.Create(outfile)
	require.NoError(t, err)
	defer f.Close()

	w := json.NewEncoder(f)
	w.SetIndent("", "  ")
	w.SetEscapeHTML(false)
	w.Encode(goldenHash)
}

const GenerateUnicodeHashes = false

func TestUnicodeHashGenerate(t *testing.T) {
	if !GenerateUnicodeHashes {
		t.Skipf("Do not generate")
	}

	golden := &testutil.GoldenTest{}
	if err := golden.DecodeFromFile("../../../mysql/collations/testdata/wiki_416c626572742045696e737465696e.gob.gz"); err != nil {
		t.Fatal(err)
	}

	var inputs [][]byte
	for _, tc := range golden.Cases {
		inputs = append(inputs, tc.Text)
	}
	GenerateGoldenCases(t, inputs, "testdata/unicode_hash_golden.json")
}

func TestUnicodeHash(t *testing.T) {
	var golden []GoldenUnicodeHash

	f, err := os.Open("testdata/unicode_hash_golden.json")
	require.NoError(t, err)

	err = json.NewDecoder(f).Decode(&golden)
	require.NoError(t, err)

	for _, tc := range golden {
		v := sqltypes.NewVarChar(string(tc.Input))

		hash1, err := unicodeHash(vXXHash, v)
		require.NoError(t, err)
		assert.Equal(t, tc.XXHash, hash1)

		hash2, err := unicodeHash(vMD5Hash, v)
		require.NoError(t, err)
		assert.Equal(t, tc.MD5, hash2)
	}
}
