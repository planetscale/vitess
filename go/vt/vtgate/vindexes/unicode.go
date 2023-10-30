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
	"fmt"
	"sync"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/vindex/collate"
	"vitess.io/vitess/go/sqltypes"
)

// Shared functions for Unicode string normalization
// for Vindexes.

func unicodeHash(hashFunc func([]byte) []byte, key sqltypes.Value) ([]byte, error) {
	collator := collatorPool.Get().(*collate.Collator)
	defer collatorPool.Put(collator)

	keyBytes, err := key.ToBytes()
	if err != nil {
		return nil, err
	}
	norm, err := normalize(collator, keyBytes)
	if err != nil {
		return nil, err
	}
	return hashFunc(norm), nil
}

func normalize(col *collate.Collator, in []byte) ([]byte, error) {
	// We cannot pass invalid UTF-8 to the collator.
	if !utf8.Valid(in) {
		return nil, fmt.Errorf("cannot normalize string containing invalid UTF-8: %q", string(in))
	}

	// Ref: http://dev.mysql.com/doc/refman/5.6/en/char.html.
	// Trailing spaces are ignored by MySQL.
	in = bytes.TrimRight(in, " ")

	// We use the collation key which can be used to
	// perform lexical comparisons.
	return col.Key(nil, in), nil
}

var collatorPool = sync.Pool{New: func() any {
	return collate.New()
}}
