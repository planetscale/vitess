package boostplan

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

type UnknownTableError struct {
	Keyspace, Table string
}

func (e *UnknownTableError) Error() string {
	if e.Keyspace == "" {
		return fmt.Sprintf("unknown table: %s", e.Table)
	}
	return fmt.Sprintf("unknown table: %s.%s", e.Keyspace, e.Table)
}

type AmbiguousTableError struct {
	Table string
}

func (e *AmbiguousTableError) Error() string {
	return fmt.Sprintf("ambiguous table: %s", e.Table)
}

type SyntaxError struct {
	Err   error
	Query string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("syntax error parsing %q: %v", e.Query, e.Err)
}

func (e *SyntaxError) Cause() error {
	return e.Err
}

type UnsupportedQueryTypeError struct {
	Query sqlparser.Statement
}

func (u *UnsupportedQueryTypeError) Error() string {
	return fmt.Sprintf("unsupported query type %T: %s", u.Query, u.Query)
}

type UnknownPublicIDError struct {
	PublicID string
}

func (u *UnknownPublicIDError) Error() string {
	return fmt.Sprintf("unknown public ID: %s", u.PublicID)
}

type UnknownQueryError struct {
	Name string
}

func (u *UnknownQueryError) Error() string {
	return fmt.Sprintf("no query named %q in current recipe", u.Name)
}

type QueryError interface {
	error
	QueryPublicID() string
}

var _ QueryError = (*QueryScopedError)(nil)

type QueryScopedError struct {
	Err   error
	Query *CachedQuery
}

func (q *QueryScopedError) Error() string {
	return fmt.Sprintf("%v (in query %q)", q.Err, q.Query.PublicId)
}

func (q *QueryScopedError) Cause() error {
	return q.Err
}

func (q *QueryScopedError) QueryPublicID() string {
	return q.Query.PublicId
}
