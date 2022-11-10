package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type UnsupportedType int

const (
	Unknown UnsupportedType = iota
	SelectColumnType
	GroupingOnLiteral
	GroupByNoAggregation
	QueryType
	SelectInto
	SelectWith
	SelectWindows
	TableExpression
	AliasedTableExpression
	JoinType
	JoinWithUsing
	ParametersInsideUnion
	OrderByNoLimit
	LimitNoOrderBy
	Offset
	ColumnsNotExpanded
	LiteralInAggregatingQuery
	DerivedTableColumnAlias
	NotAliasedExpression
	AggregationInWhere
	AggregationInComplexExpression
	EvalEngineNotSupported
	DualTable
	ParameterLocation
	ParameterLocationCompare
	ParameterNotEqual
	JoinWithoutPredicates
)

type (
	UnsupportedError struct {
		AST  sqlparser.SQLNode
		Type UnsupportedType
	}
	NoPrimaryKeyError struct {
		Keyspace, Table string
		Spec            *sqlparser.TableSpec
	}
	UnknownColumnsError struct {
		Keyspace, Table string
		Columns         []string
	}
	BugError struct {
		message string
	}
)

func NewBug(message string) error {
	return &BugError{message: message}
}

func (n *UnsupportedError) Error() string {
	var sb strings.Builder
	sb.WriteString("query not supported by boost: ")
	switch n.Type {
	case Unknown:
		sb.WriteString("unknown problem")
	case SelectColumnType:
		fmt.Fprintf(&sb, "column reference of unknown type in select: %s", sqlparser.CanonicalString(n.AST))
	case GroupingOnLiteral:
		fmt.Fprintf(&sb, "query contains literal in group by clause: %s", sqlparser.CanonicalString(n.AST))
	case GroupByNoAggregation:
		fmt.Fprintf(&sb, "query contains group by with no aggregation: %s", sqlparser.CanonicalString(n.AST))
	case QueryType:
		fmt.Fprintf(&sb, "unsupported query type: %s", sqlparser.CanonicalString(n.AST))
	case SelectInto:
		fmt.Fprintf(&sb, "select uses unsupported into: %s", sqlparser.CanonicalString(n.AST))
	case SelectWith:
		fmt.Fprintf(&sb, "select uses unsupported with: %s", sqlparser.CanonicalString(n.AST))
	case SelectWindows:
		fmt.Fprintf(&sb, "select uses unsupported windows: %s", sqlparser.CanonicalString(n.AST))
	case TableExpression:
		fmt.Fprintf(&sb, "query contains an unsupported table expression: %s", sqlparser.CanonicalString(n.AST))
	case AliasedTableExpression:
		fmt.Fprintf(&sb, "query contains an unsupported aliased table expression: %s", sqlparser.CanonicalString(n.AST))
	case JoinType:
		fmt.Fprintf(&sb, "query uses an unsupported join type: %s", sqlparser.CanonicalString(n.AST))
	case JoinWithUsing:
		fmt.Fprintf(&sb, "query uses unsupported using condition on join: %s", sqlparser.CanonicalString(n.AST))
	case ParametersInsideUnion:
		fmt.Fprintf(&sb, "query uses unsupported parameters inside a union: %s", sqlparser.CanonicalString(n.AST))
	case OrderByNoLimit:
		fmt.Fprintf(&sb, "query contains order by with no limit: %s", sqlparser.CanonicalString(n.AST))
	case LimitNoOrderBy:
		fmt.Fprintf(&sb, "query contains limit with no order by: %s", sqlparser.CanonicalString(n.AST))
	case Offset:
		fmt.Fprintf(&sb, "query contains offset: %s", sqlparser.CanonicalString(n.AST))
	case ColumnsNotExpanded:
		fmt.Fprintf(&sb, "columns in select * are not expanded: %s", sqlparser.CanonicalString(n.AST))
	case LiteralInAggregatingQuery:
		fmt.Fprintf(&sb, "unsupported use of literal in aggregating query: %s", sqlparser.CanonicalString(n.AST))
	case DerivedTableColumnAlias:
		fmt.Fprintf(&sb, "derived table with a column alias is not supported: %s", sqlparser.CanonicalString(n.AST))
	case NotAliasedExpression:
		fmt.Fprintf(&sb, "column in query is not an aliased expression: %s", sqlparser.CanonicalString(n.AST))
	case AggregationInWhere:
		fmt.Fprintf(&sb, "unsupported use of aggregation expression in where clause: %s", sqlparser.CanonicalString(n.AST))
	case AggregationInComplexExpression:
		fmt.Fprintf(&sb, "aggregation cannot be inside complex expression: %s", sqlparser.CanonicalString(n.AST))
	case EvalEngineNotSupported:
		fmt.Fprintf(&sb, "unsupported expression by the evaluation engine: %s", sqlparser.CanonicalString(n.AST))
	case DualTable:
		fmt.Fprintf(&sb, "select without an explicit table is not supported")
	case ParameterLocation:
		fmt.Fprintf(&sb, "unsupported parameter location: %s", sqlparser.CanonicalString(n.AST))
	case ParameterNotEqual:
		fmt.Fprintf(&sb, "parameter %s can only be compared for equality", sqlparser.CanonicalString(n.AST))
	case ParameterLocationCompare:
		fmt.Fprintf(&sb, "parameter %s needs to be used in a comparison expression", sqlparser.CanonicalString(n.AST))
	case JoinWithoutPredicates:
		fmt.Fprintf(&sb, "join without predicates is not supported")
	}

	return sb.String()
}

func (u *UnknownColumnsError) Error() string {
	return fmt.Sprintf("unknown column(s) %+v in table %s.%s", u.Columns, u.Keyspace, u.Table)
}

func (n *NoPrimaryKeyError) Error() string {
	return fmt.Sprintf("table %s.%s has no primary key", n.Keyspace, n.Table)
}

func (b *BugError) Error() string {
	return fmt.Sprintf("[BUG] %s", b.message)
}
