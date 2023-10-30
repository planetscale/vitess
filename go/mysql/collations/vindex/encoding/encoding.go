// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package encoding defines an interface for character encodings, such as Shift
// JIS and Windows 1252, that can convert to and from UTF-8.
//
// Encoding implementations are provided in other packages, such as
// vitess.io/vitess/go/mysql/collations/vindex/encoding/charmap and
// vitess.io/vitess/go/mysql/collations/vindex/encoding/japanese.
package encoding // import "vitess.io/vitess/go/mysql/collations/vindex/encoding"

import (
	"io"

	"vitess.io/vitess/go/mysql/collations/vindex/transform"
)

// TODO:
// - There seems to be some inconsistency in when decoders return errors
//   and when not. Also documentation seems to suggest they shouldn't return
//   errors at all (except for UTF-16).
// - Encoders seem to rely on or at least benefit from the input being in NFC
//   normal form. Perhaps add an example how users could prepare their output.

// Encoding is a character set encoding that can be transformed to and from
// UTF-8.
type Encoding interface {
	// NewDecoder returns a Decoder.
	NewDecoder() *Decoder

	// NewEncoder returns an Encoder.
	NewEncoder() *Encoder
}

// A Decoder converts bytes to UTF-8. It implements transform.Transformer.
//
// Transforming source bytes that are not of that encoding will not result in an
// error per se. Each byte that cannot be transcoded will be represented in the
// output by the UTF-8 encoding of '\uFFFD', the replacement rune.
type Decoder struct {
	transform.Transformer

	// This forces external creators of Decoders to use names in struct
	// initializers, allowing for future extendibility without having to break
	// code.
	_ struct{}
}

// Bytes converts the given encoded bytes to UTF-8. It returns the converted
// bytes or nil, err if any error occurred.
func (d *Decoder) Bytes(b []byte) ([]byte, error) {
	b, _, err := transform.Bytes(d, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// String converts the given encoded string to UTF-8. It returns the converted
// string or "", err if any error occurred.
func (d *Decoder) String(s string) (string, error) {
	s, _, err := transform.String(d, s)
	if err != nil {
		return "", err
	}
	return s, nil
}

// Reader wraps another Reader to decode its bytes.
//
// The Decoder may not be used for any other operation as long as the returned
// Reader is in use.
func (d *Decoder) Reader(r io.Reader) io.Reader {
	return transform.NewReader(r, d)
}

// An Encoder converts bytes from UTF-8. It implements transform.Transformer.
//
// Each rune that cannot be transcoded will result in an error. In this case,
// the transform will consume all source byte up to, not including the offending
// rune. Transforming source bytes that are not valid UTF-8 will be replaced by
// `\uFFFD`. To return early with an error instead, use transform.Chain to
// preprocess the data with a UTF8Validator.
type Encoder struct {
	transform.Transformer

	// This forces external creators of Encoders to use names in struct
	// initializers, allowing for future extendibility without having to break
	// code.
	_ struct{}
}

// Bytes converts bytes from UTF-8. It returns the converted bytes or nil, err if
// any error occurred.
func (e *Encoder) Bytes(b []byte) ([]byte, error) {
	b, _, err := transform.Bytes(e, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// String converts a string from UTF-8. It returns the converted string or
// "", err if any error occurred.
func (e *Encoder) String(s string) (string, error) {
	s, _, err := transform.String(e, s)
	if err != nil {
		return "", err
	}
	return s, nil
}

// Writer wraps another Writer to encode its UTF-8 output.
//
// The Encoder may not be used for any other operation as long as the returned
// Writer is in use.
func (e *Encoder) Writer(w io.Writer) io.Writer {
	return transform.NewWriter(w, e)
}
