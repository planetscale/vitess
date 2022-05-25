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
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ MultiColumn = (*MultiColBit)(nil)

type MultiColBit struct {
	name       string
	cost       int
	noOfCols   int
	columnVdx  map[int]Hashing
	columnBits map[int]int
}

const (
	paramColumnBits = "column_bits"
)

// NewMultiColBit creates a new MultiColBit.
func NewMultiColBit(name string, m map[string]string) (Vindex, error) {
	colCount, err := getColumnCount(m)
	if err != nil {
		return nil, err
	}
	columnBits, err := getColumnBits(m, colCount)
	if err != nil {
		return nil, err
	}
	columnVdx, vindexCost, err := getColumnVindex(m, colCount)
	if err != nil {
		return nil, err
	}

	return &MultiColBit{
		name:       name,
		cost:       vindexCost,
		noOfCols:   colCount,
		columnVdx:  columnVdx,
		columnBits: columnBits,
	}, nil
}

func (m *MultiColBit) String() string {
	return m.name
}

func (m *MultiColBit) Cost() int {
	return m.cost
}

func (m *MultiColBit) IsUnique() bool {
	return true
}

func (m *MultiColBit) NeedsVCursor() bool {
	return false
}

// NewKeyRangeFromBitPrefix creates a keyspace range from a bitwise prefix of keyspace id.
func NewKeyRangeFromBitPrefix(beginPrefixUint uint64, length uint) key.Destination {
	if length < 1 {
		return key.DestinationAllShards{}
	}
	// the prefix maps to a keyspace range corresponding to its value and plus one.
	// that is [ keyspace_id, keyspace_id + 1 ).
	// This code is a bit crazy, but it is to get the byte padding right
	//   since ksIds in Vitess are expressed in byte arrays
	begin := make([]byte, 8)
	end := make([]byte, 8)

	// Determine how many bytes we need to hold the bits we are using
	byteLength := (length / 8)
	if (length % 8) > 0 {
		byteLength++
	}

	// Shift over to least significant, so we can operate on it
	beginPrefixUint = beginPrefixUint >> (64 - length)
	// Add one for the end of the range
	// if we overflow length bits, set to zero, will handle later
	endPrefixUint := ((beginPrefixUint + 1) % (2 << (length - 1)))

	// If not on a byte boundary, shift left to the closest one
	if (length % 8) != 0 {
		endPrefixUint = endPrefixUint << (8 - (length % 8))
		beginPrefixUint = beginPrefixUint << (8 - (length % 8))
	}

	if endPrefixUint == 0 {
		// Handle the overflow; we are at the end of the keyrange
		end = nil
	} else {
		binary.BigEndian.PutUint64(end, endPrefixUint)
		// Only want the significant bytes
		end = end[8-byteLength : 8]
	}

	binary.BigEndian.PutUint64(begin, beginPrefixUint)
	return key.DestinationKeyRange{
		KeyRange: &topodatapb.KeyRange{
			Start: begin[8-byteLength : 8],
			End:   end,
		},
	}
}

func (m *MultiColBit) Map(_ VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(rowsColValues))
	for _, colValues := range rowsColValues {
		partialBits, ksidUint, err := m.mapKsid(colValues)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		if partialBits < 64 {
			out = append(out, NewKeyRangeFromBitPrefix(ksidUint, partialBits))
			continue
		}
		ksid := make([]byte, 8)
		binary.BigEndian.PutUint64(ksid, ksidUint)
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

func (m *MultiColBit) Verify(_ VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(rowsColValues))
	for idx, colValues := range rowsColValues {
		_, ksidUint, err := m.mapKsid(colValues)
		if err != nil {
			return nil, err
		}
		ksid := make([]byte, 8)
		binary.BigEndian.PutUint64(ksid, ksidUint)
		out = append(out, bytes.Equal(ksid, ksids[idx]))
	}
	return out, nil
}

func (m *MultiColBit) PartialVindex() bool {
	return true
}

func (m *MultiColBit) mapKsid(colValues []sqltypes.Value) (uint, uint64, error) {
	if m.noOfCols < len(colValues) {
		// wrong number of column values were passed
		return 0, 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] wrong number of column values were passed: maximum allowed %d, got %d", m.noOfCols, len(colValues))
	}

	var ksidUint uint64
	totalBits := 64
	for idx, colVal := range colValues {
		lksid, err := m.columnVdx[idx].Hash(colVal)
		if err != nil {
			return 0, 0, err
		}
		lksidUint := binary.BigEndian.Uint64(lksid)
		numBitsToAdd := m.columnBits[idx]
		totalBits = totalBits - numBitsToAdd
		ksidUint |= lksidUint >> (64 - numBitsToAdd) << totalBits
	}
	return uint(64 - totalBits), ksidUint, nil
}

func init() {
	Register("multicolbit", NewMultiColBit)
}

func getColumnBits(m map[string]string, colCount int) (map[int]int, error) {
	var colBitStr []string
	colBitsStr, ok := m[paramColumnBits]
	if ok {
		colBitStr = strings.Split(colBitsStr, ",")
	}
	if len(colBitStr) > colCount {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of column bits provided are more than column count in the parameter '%s'", paramColumnBits)
	}
	// validate bit count
	bitsUsed := 0
	columnBits := make(map[int]int, colCount)
	for idx, bitStr := range colBitStr {
		if bitStr == "" {
			continue
		}
		colBits, err := strconv.Atoi(bitStr)
		if err != nil {
			return nil, err
		}
		bitsUsed = bitsUsed + colBits
		columnBits[idx] = colBits
	}
	pendingCol := colCount - len(columnBits)
	remainingBits := 64 - bitsUsed
	if pendingCol > remainingBits {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column bit count exceeds the keyspace id length (total bit count cannot exceed 64 bits) in the parameter '%s'", paramColumnBits)
	}
	if pendingCol <= 0 {
		return columnBits, nil
	}
	for idx := 0; idx < colCount; idx++ {
		if _, defined := columnBits[idx]; defined {
			continue
		}
		bitsToAssign := int(math.Ceil(float64(remainingBits) / float64(pendingCol)))
		columnBits[idx] = bitsToAssign
		remainingBits = remainingBits - bitsToAssign
		pendingCol--
	}
	return columnBits, nil
}
