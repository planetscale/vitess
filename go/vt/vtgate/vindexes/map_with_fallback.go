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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn = (*MapWithFallback)(nil)
	_ Hashing      = (*MapWithFallback)(nil)
)

// MapFallbackLookupTable stores the mapping of keys.
type MapFallbackLookupTable map[uint64]uint64

// MapWithFallback does a lookup from a small set of (integer) value to
// keyspace id mappings provided in the vschema itself;  and then falls
// back to using the specified hash function for values that there is
// no mapping for
type MapWithFallback struct {
	name     string
	hashVdx  Hashing
	valueMap MapFallbackLookupTable
}

func init() {
	Register("map_with_fallback", NewMapWithFallback)
}

// NewMapWithFallback creates a MapWithFallback vindex.
func NewMapWithFallback(name string, params map[string]string) (Vindex, error) {
	// We just use xxhash blindly;  we could make this configurable
	vindex, err := CreateVindex("xxhash", name+"_hash", map[string]string{})
	if err != nil {
		return nil, err
	}
	hashVindex, _ := vindex.(Hashing) // We know this will not fail

	mapString, ok := params["map"]
	if !ok {
		return nil, errors.New("MapWithFallback: Could not find `map` param in vschema")
	}

	// Map is declared as "map": "1:2,2:3,4:5", since the vschema parsing
	// does not support nested json object for parameters
	valueMap := make(map[uint64]uint64)
	for _, s := range strings.Split(mapString, ",") {
		slice := strings.Split(s, ":")
		if len(slice) != 2 {
			return nil, errors.New("MapWithFallback: `map` format incorrect")
		}
		newK, err := strconv.ParseUint(slice[0], 10, 64)
		if err != nil {
			return nil, errors.New("MapWithFallback: `map` format incorrect for key")
		}
		newV, err := strconv.ParseUint(slice[1], 10, 64)
		if err != nil {
			return nil, errors.New("MapWithFallback: `map` format incorrect for value")
		}
		valueMap[newK] = newV
	}

	return &MapWithFallback{
		name:     name,
		hashVdx:  hashVindex,
		valueMap: valueMap,
	}, nil
}

// String returns the name of the vindex.
func (vind *MapWithFallback) String() string {
	return vind.name
}

// Cost returns the cost of this vindex as 1.
func (*MapWithFallback) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *MapWithFallback) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *MapWithFallback) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids and ksids match.
func (vind *MapWithFallback) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(ids))
	for i, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			return nil, err
		}
		out = append(out, bytes.Equal(ksid, ksids[i]))
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *MapWithFallback) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

func (vind *MapWithFallback) Hash(id sqltypes.Value) ([]byte, error) {
	num, err := evalengine.ToUint64(id)
	if err != nil {
		return nil, err
	}
	lookupNum, ok := vind.valueMap[num]
	if !ok {
		// Not in lookup, us fallback hash
		return vind.hashVdx.Hash(id)
	}

	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], lookupNum)
	return keybytes[:], nil
}
