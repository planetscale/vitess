// Package unicode2 generates the Unicode tables in core.
package unicode2

// This package is defined here, instead of core, as Go does not allow any
// standard packages to have non-standard imports, even if imported in files
// with a build ignore tag.

//go:generate go run ../tools/genunicode/gen.go
