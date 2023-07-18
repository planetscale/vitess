package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/sqlparser"
)

//enumcheck:exhaustive
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
	JoinWithoutPredicates
	JoinPredicates
	MultipleIn
	Aggregation
	NoFullGroupBy
	Lock
	SubQuery
	NonConstantExpression
	NoIndexableColumn
	RangeAndAggregation
	PostAndAggregation
	OffsetBindParameter
)

//enumcheck:exhaustive
type UnsupportedRangeType int

const (
	UnknownRangeError UnsupportedRangeType = iota
	RangeEqualityOperator
	RangeSameDirection
)

type (
	UnsupportedError struct {
		AST  sqlparser.SQLNode
		Type UnsupportedType
	}
	NoUniqueKeyError struct {
		Keyspace, Table string
		Spec            *sqlparser.TableSpec
	}
	UnknownColumnsError struct {
		Keyspace, Table string
		Columns         []string
	}
	UnsupportedRangeError struct {
		Type    UnsupportedRangeType
		Columns []string
	}
)

func (n *UnsupportedError) ast() string {
	switch ast := n.AST.(type) {
	case *sqlparser.Offset:
		return sqlparser.CanonicalString(ast.Original)
	default:
		return sqlparser.CanonicalString(ast)
	}
}

func (n *UnsupportedError) Error() string {
	var sb strings.Builder
	sb.WriteString("query not supported by boost: ")
	switch n.Type {
	case Unknown:
		sb.WriteString("unknown problem")
	case SelectColumnType:
		fmt.Fprintf(&sb, "column reference of unknown type in select: %s", n.ast())
	case GroupingOnLiteral:
		fmt.Fprintf(&sb, "query contains literal in group by clause: %s", n.ast())
	case GroupByNoAggregation:
		fmt.Fprintf(&sb, "query contains group by with no aggregation: %s", n.ast())
	case QueryType:
		fmt.Fprintf(&sb, "unsupported query type: %s", n.ast())
	case SelectInto:
		fmt.Fprintf(&sb, "select uses unsupported into: %s", n.ast())
	case SelectWith:
		fmt.Fprintf(&sb, "select uses unsupported with: %s", n.ast())
	case SelectWindows:
		fmt.Fprintf(&sb, "select uses unsupported windows: %s", n.ast())
	case TableExpression:
		fmt.Fprintf(&sb, "query contains an unsupported table expression: %s", n.ast())
	case AliasedTableExpression:
		fmt.Fprintf(&sb, "query contains an unsupported aliased table expression: %s", n.ast())
	case JoinType:
		fmt.Fprintf(&sb, "query uses an unsupported join type: %s", n.ast())
	case JoinWithUsing:
		fmt.Fprintf(&sb, "query uses unsupported using condition on join: %s", n.ast())
	case ParametersInsideUnion:
		fmt.Fprintf(&sb, "query uses unsupported parameters inside a union: %s", n.ast())
	case OrderByNoLimit:
		fmt.Fprintf(&sb, "query contains order by with no limit: %s", n.ast())
	case LimitNoOrderBy:
		fmt.Fprintf(&sb, "query contains limit with no order by: %s", n.ast())
	case Offset:
		fmt.Fprintf(&sb, "query contains offset: %s", n.ast())
	case ColumnsNotExpanded:
		fmt.Fprintf(&sb, "columns in select * are not expanded: %s", n.ast())
	case LiteralInAggregatingQuery:
		fmt.Fprintf(&sb, "unsupported use of literal in aggregating query: %s", n.ast())
	case DerivedTableColumnAlias:
		fmt.Fprintf(&sb, "derived table with a column alias is not supported: %s", n.ast())
	case NotAliasedExpression:
		fmt.Fprintf(&sb, "column in query is not an aliased expression: %s", n.ast())
	case AggregationInWhere:
		fmt.Fprintf(&sb, "unsupported use of aggregation expression in where clause: %s", n.ast())
	case AggregationInComplexExpression:
		fmt.Fprintf(&sb, "aggregation cannot be inside complex expression: %s", n.ast())
	case EvalEngineNotSupported:
		fmt.Fprintf(&sb, "unsupported expression by the evaluation engine: %s", n.ast())
	case DualTable:
		fmt.Fprintf(&sb, "select without an explicit table is not supported")
	case ParameterLocation:
		fmt.Fprintf(&sb, "unsupported parameter location: %s", n.ast())
	case ParameterLocationCompare:
		fmt.Fprintf(&sb, "parameter %s needs to be used in a comparison expression", n.ast())
	case JoinWithoutPredicates:
		fmt.Fprintf(&sb, "join without predicates is not supported")
	case JoinPredicates:
		fmt.Fprintf(&sb, "join predicates have to be in the form <tbl1.col> = <tbl2.col>, was '%s'", n.ast())
	case MultipleIn:
		fmt.Fprintf(&sb, "multiple IN() clauses for a parameter are not supported: %v", n.ast())
	case Aggregation:
		fmt.Fprintf(&sb, "group aggregation function '%s' is not supported", n.ast())
	case NoFullGroupBy:
		fmt.Fprintf(&sb, "non aggregated column '%s' is not part of group by", n.ast())
	case Lock:
		sel := n.AST.(*sqlparser.Select)
		fmt.Fprintf(&sb, "row locking with '%s' is not supported", sel.Lock.ToString())
	case SubQuery:
		fmt.Fprintf(&sb, "subqueries are not supported: %s", n.ast())
	case NonConstantExpression:
		fmt.Fprintf(&sb, "non constant expression in query: %s", n.ast())
	case NoIndexableColumn:
		fmt.Fprintf(&sb, "none of the provided filter operators can be indexed")
	case RangeAndAggregation:
		fmt.Fprintf(&sb, "the ranged query for parameter %s on top of an aggregation must be grouped by that parameter", n.ast())
	case PostAndAggregation:
		fmt.Fprintf(&sb, "the explicit grouping for %s cannot be computed with the given filters", n.ast())
	case OffsetBindParameter:
		fmt.Fprintf(&sb, "offset bind parameter used on %s", n.ast())
	}

	return sb.String()
}

func (u *UnknownColumnsError) Error() string {
	return fmt.Sprintf("unknown column(s) %+v in table %s.%s", u.Columns, u.Keyspace, u.Table)
}

func (n *NoUniqueKeyError) Error() string {
	return fmt.Sprintf("table %s.%s has no unique non nullable key", n.Keyspace, n.Table)
}

func (n *UnsupportedRangeError) Error() string {
	var sb strings.Builder
	sb.WriteString("query not supported by boost: ")
	switch n.Type {
	case UnknownRangeError:
		sb.WriteString("unknown range problem")
	case RangeEqualityOperator:
		fmt.Fprintf(&sb, "equality operator used with range operator on column %s", sqlescape.EscapeID(n.Columns[0]))
	case RangeSameDirection:
		fmt.Fprintf(&sb, "multiple range operators with the same direction on column %s", sqlescape.EscapeID(n.Columns[0]))
	}

	return sb.String()
}
