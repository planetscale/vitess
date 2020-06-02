/*
Copyright 2020 The Vitess Authors.

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
	"encoding/binary"
	"strconv"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
)

var (
	_ MultiColumn = (*CustomRegionJSON)(nil)
)

func init() {
	Register("custom_region_json", NewCustomRegionJSON)
}

type regionMap map[string]int64

// CustomRegionJSON is a multi-column unique vindex
// The first column is used to lookup the prefix part of the keyspace id, the second column is hashed,
// and the two values are combined to produce the keyspace id.
// RegionJson can be used for geo-partitioning because the first column can denote a region,
// and it will dictate the shard range for that region.
type CustomRegionJSON struct {
	name        string
	regionMap   regionMap
	regionBytes int
}

// NewCustomRegionJSON creates a CustomRegionJson vindex.
// The supplied map should have the "country":"code" mapping
func NewCustomRegionJSON(name string, m map[string]string) (Vindex, error) {
	// input map m contains params for the vindex
	// we are going to interpret it as "country":"code"
	// no other params are allowed
	rmap := make(regionMap)
	for country, v := range m {
		code, err := strconv.ParseInt(v, 10, 64)
		// ignoring rows with errors
		if err == nil {
			rmap[country] = code
		} else {
			log.Errorf("custom RegionJson vindex: error parsing code %v for country %v:%v", v, country, err)
		}
	}
	return &CustomRegionJSON{
		name:        name,
		regionMap:   rmap,
		regionBytes: 1, // always 1 byte
	}, nil
}

// String returns the name of the vindex.
func (rv *CustomRegionJSON) String() string {
	return rv.name
}

// Cost returns the cost of this index as 1.
func (rv *CustomRegionJSON) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (rv *CustomRegionJSON) IsUnique() bool {
	return true
}

// Map satisfies MultiColumn.
func (rv *CustomRegionJSON) Map(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(rowsColValues))
	for _, row := range rowsColValues {
		if len(row) != 2 {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		// Compute hash.
		hn, err := evalengine.ToUint64(row[0])
		if err != nil {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		h := vhash(hn)

		rn, ok := rv.regionMap[row[1].ToString()]
		if !ok {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		r := make([]byte, 2)
		binary.BigEndian.PutUint16(r, uint16(rn))

		// Concatenate and add to destinations.
		if rv.regionBytes == 1 {
			r = r[1:]
		}
		dest := append(r, h...)
		destinations = append(destinations, key.DestinationKeyspaceID(dest))
	}
	return destinations, nil
}

// Verify satisfies MultiColumn
func (rv *CustomRegionJSON) Verify(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := rv.Map(vcursor, rowsColValues)
	for i, dest := range destinations {
		destksid, ok := dest.(key.DestinationKeyspaceID)
		if !ok {
			continue
		}
		result[i] = bytes.Equal([]byte(destksid), ksids[i])
	}
	return result, nil
}

// NeedsVCursor satisfies the Vindex interface.
func (rv *CustomRegionJSON) NeedsVCursor() bool {
	return false
}
