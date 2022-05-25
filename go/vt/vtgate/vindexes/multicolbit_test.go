/*
Copyright 2021 The Vitess Authors.

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

package vindexes

import (
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiColBitMisc(t *testing.T) {
	vindex, err := CreateVindex("multicolbit", "multicolbit", map[string]string{
		"column_count": "3",
		"column_bits":  "4,40,20",
	})
	require.NoError(t, err)

	multiColBitVdx, isMultiColBitVdx := vindex.(*MultiColBit)
	assert.True(t, isMultiColBitVdx)

	assert.Equal(t, 3, multiColBitVdx.Cost())
	assert.Equal(t, "multicolbit", multiColBitVdx.String())
	assert.True(t, multiColBitVdx.IsUnique())
	assert.False(t, multiColBitVdx.NeedsVCursor())
	assert.True(t, multiColBitVdx.PartialVindex())
}

func TestMultiColBitMap(t *testing.T) {
	vindex, err := CreateVindex("multicolbit", "multicolbit", map[string]string{
		"column_count": "3",
		"column_bits":  "4,40,20",
	})
	require.NoError(t, err)
	multiColBit := vindex.(MultiColumn)

	got, err := multiColBit.Map(nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// only one column provided, partial column for key range mapping.
		sqltypes.NewInt64(1),
	}, {
		// only two columns provided, partial column for key range mapping.
		sqltypes.NewInt64(1), sqltypes.NewInt64(2),
	}, {
		// Invalid column value type.
		sqltypes.NewVarBinary("abcd"), sqltypes.NewInt64(256), sqltypes.NewInt64(256),
	}, {
		// Invalid column value type.
		sqltypes.NewInt64(256), sqltypes.NewInt64(256), sqltypes.NewVarBinary("abcd"),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID("\x11\x66\xb4\x0b\x44\xa1\x66\xb4"),
		key.DestinationKeyspaceID("\x21\x66\xb4\x0b\x44\xa1\x66\xb4"),
		key.DestinationKeyspaceID("\xd1\x66\xb4\x0b\x44\xa1\x66\xb4"),
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x10"), End: []byte("\x20")}},
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x10\x6e\x7e\xa2\x2c\xe0"), End: []byte("\x10\x6e\x7e\xa2\x2c\xf0")}},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}

func TestMultiColBitMap2(t *testing.T) {
	vindex, err := CreateVindex("multicolbit", "multicolbit", map[string]string{
		"column_count": "3",
		"column_bits":  "36,24,4",
	})
	require.NoError(t, err)
	multiColBit := vindex.(MultiColumn)

	got, err := multiColBit.Map(nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// only one column provided, partial column for key range mapping.
		sqltypes.NewInt64(1),
	}, {
		// only two columns provided, partial column for key range mapping.
		sqltypes.NewInt64(1), sqltypes.NewInt64(2),
	}, {
		// Invalid column value type.
		sqltypes.NewVarBinary("abcd"), sqltypes.NewInt64(256), sqltypes.NewInt64(256),
	}, {
		// Invalid column value type.
		sqltypes.NewInt64(256), sqltypes.NewInt64(256), sqltypes.NewVarBinary("abcd"),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID("\x16\x6b\x40\xb4\x41\x66\xb4\x01"),
		key.DestinationKeyspaceID("\x25\x4e\x88\x2e\x61\x66\xb4\x01"),
		key.DestinationKeyspaceID("\xdd\x7c\x0b\xbd\x61\x66\xb4\x01"),
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x16\x6b\x40\xb4\x40"), End: []byte("\x16\x6b\x40\xb4\x50")}},
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x16\x6b\x40\xb4\x40\x6e\x7e\xa0"), End: []byte("\x16\x6b\x40\xb4\x40\x6e\x7e\xb0")}},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}

func TestNewKeyRangeFromBitPrefix(t *testing.T) {
	got := NewKeyRangeFromBitPrefix(0xffffffffffffffff, 8)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\xff"), End: nil}}, got)

	got = NewKeyRangeFromBitPrefix(0xffffffffffffffff, 15)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\xff\xfe"), End: nil}}, got)

	got = NewKeyRangeFromBitPrefix(0xffffffffffffffff, 16)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\xff\xff"), End: nil}}, got)

	got = NewKeyRangeFromBitPrefix(0xeeeeeeeeeeeeeeee, 16)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\xee\xee"), End: []byte("\xee\xef")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 16)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x01")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 15)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x02")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 14)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x04")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 13)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x08")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 12)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x10")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 11)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x20")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 10)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x40")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 9)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00\x00"), End: []byte("\x00\x80")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 8)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00"), End: []byte("\x01")}}, got)

	got = NewKeyRangeFromBitPrefix(0x0000000000000000, 7)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x00"), End: []byte("\x02")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 15)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34"), End: []byte("\x12\x36")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 14)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34"), End: []byte("\x12\x38")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 13)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x30"), End: []byte("\x12\x38")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 12)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x30"), End: []byte("\x12\x40")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 11)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x20"), End: []byte("\x12\x40")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 10)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x00"), End: []byte("\x12\x40")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 9)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x00"), End: []byte("\x12\x80")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 8)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12"), End: []byte("\x13")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234000000000000, 7)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12"), End: []byte("\x14")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 16)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34"), End: []byte("\x12\x35")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 17)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x00"), End: []byte("\x12\x34\x80")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 18)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x40"), End: []byte("\x12\x34\x80")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 19)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x40"), End: []byte("\x12\x34\x60")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 20)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x50"), End: []byte("\x12\x34\x60")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 21)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x50"), End: []byte("\x12\x34\x58")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 22)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x54"), End: []byte("\x12\x34\x58")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 23)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x56"), End: []byte("\x12\x34\x58")}}, got)

	got = NewKeyRangeFromBitPrefix(0x1234567800000000, 24)
	assert.Equal(t, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x12\x34\x56"), End: []byte("\x12\x34\x57")}}, got)
}
