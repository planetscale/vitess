/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func createMapWithFallbackVindex(m map[string]string) (SingleColumn, error) {
	vindex, err := CreateVindex("map_with_fallback", "map_with_fallback_name", m)
	if err != nil {
		return nil, err
	}
	return vindex.(SingleColumn), nil
}

func TestMapWithFallbackVdx(t *testing.T) {
	mapWithFallbackVdx, err := createMapWithFallbackVindex(map[string]string{"map": "1:2,3:4,5:6"})
	require.NoError(t, err)
	assert.Equal(t, 1, mapWithFallbackVdx.Cost())
	assert.Equal(t, "map_with_fallback_name", mapWithFallbackVdx.String())
	assert.True(t, mapWithFallbackVdx.IsUnique())
	assert.False(t, mapWithFallbackVdx.NeedsVCursor())

	// Bad format tests
	mapWithFallbackVdx, err = createMapWithFallbackVindex(map[string]string{"map": "1:2,3:4,5:6:8,10:11"})
	require.EqualError(t, err, "MapWithFallback: `map` format incorrect")

	// Letters in key or value not allowed
	mapWithFallbackVdx, err = createMapWithFallbackVindex(map[string]string{"map": "1:a"})
	require.EqualError(t, err, "MapWithFallback: `map` format incorrect for value")
	mapWithFallbackVdx, err = createMapWithFallbackVindex(map[string]string{"map": "a:1"})
	require.EqualError(t, err, "MapWithFallback: `map` format incorrect for key")
}

// Test mapping of vindex, both for specified map keys and underlying xxhash
func TestMapWithFallbackMap(t *testing.T) {
	mapWithFallbackVdx, err := createMapWithFallbackVindex(map[string]string{"map": "1:2,3:4,4:5,5:6,6:7,7:8,8:9,10:18446744073709551615"})
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := mapWithFallbackVdx.Map(context.Background(), nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewFloat64(1.1),
		sqltypes.NewVarChar("test1"),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
		sqltypes.NewInt64(7),
		sqltypes.NewInt64(8),
		sqltypes.NewInt64(10),
		sqltypes.NULL,
	})
	require.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
		key.DestinationKeyspaceID([]byte("\x8b\x59\x80\x16\x62\xb5\x21\x60")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x04")),
		key.DestinationNone{},
		key.DestinationNone{}, // strings do not map
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x05")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x06")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x07")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x08")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x09")),
		key.DestinationKeyspaceID([]byte("\xff\xff\xff\xff\xff\xff\xff\xff")),
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map()\ngot: %+v\nwant: %+v", got, want)
	}
}

func TestMapWithFallbackVerify(t *testing.T) {
	mapWithFallbackVdx, err := createMapWithFallbackVindex(map[string]string{"map": "1:2,3:4,4:5,5:6,6:7,7:8,8:9,10:18446744073709551615"})
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := mapWithFallbackVdx.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(11), sqltypes.NewInt64(10)}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"), []byte("\x8b\x59\x80\x16\x62\xb5\x21\x60"), []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), []byte("\xff\xff\xff\xff\xff\xff\xff\xff")})
	require.NoError(t, err)
	want := []bool{true, true, false, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Verify(match): %v, want %v", got, want)
	}

	// Failure test
	_, err = mapWithFallbackVdx.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	require.EqualError(t, err, "could not parse value: 'aa'")
}
