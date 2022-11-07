package operators

import (
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (t *Table) MarshalJSON() ([]byte, error) {
	out := struct {
		Type      string
		TableName string
		Columns   []string
	}{
		Type:      "Table",
		TableName: t.Keyspace + "." + t.TableName,
	}

	for _, col := range t.VColumns {
		colDescr := fmt.Sprintf("%s:%s", col.Name, col.Type.String())
		if sqltypes.IsText(col.Type) {
			colDescr += ":" + col.CollationName
		}
		out.Columns = append(out.Columns, colDescr)
	}

	return json.Marshal(out)
}

func (j *Join) MarshalJSON() ([]byte, error) {
	out := struct {
		Type      string
		Predicate string
	}{
		Type:      "Join",
		Predicate: sqlparser.String(j.Predicates),
	}
	if !j.Inner {
		out.Type = "LeftJoin"
	}

	return json.Marshal(out)
}

func (f *Filter) MarshalJSON() ([]byte, error) {
	out := struct {
		Type       string
		Predicates string
	}{
		Type:       "Filter",
		Predicates: sqlparser.String(f.Predicates),
	}

	return json.Marshal(out)
}

func (g *GroupBy) MarshalJSON() ([]byte, error) {
	out := struct {
		Type         string
		Grouping     []string
		Aggregations []string
	}{
		Type: "Group By",
	}

	for _, column := range g.Grouping {
		out.Grouping = append(out.Grouping, column.Name)
	}
	for _, aggr := range g.Aggregations {
		out.Aggregations = append(out.Aggregations, sqlparser.String(aggr))
	}
	return json.Marshal(out)
}

func (p *Project) MarshalJSON() ([]byte, error) {
	out := struct {
		Type    string
		Columns []string
	}{
		Type: "Project",
	}

	for _, column := range p.Columns {
		name, details := column.Explain()
		out.Columns = append(out.Columns, fmt.Sprintf("%s as %s", details, name))
	}

	return json.Marshal(out)
}

func (v *View) MarshalJSON() ([]byte, error) {
	out := struct {
		Type   string
		Params []string
	}{
		Type: "View",
	}

	for _, p := range v.Parameters {
		out.Params = append(out.Params, p.key.Name)
	}

	return json.Marshal(out)
}

func (col *Column) MarshalJSON() ([]byte, error) {
	out := struct {
		AST    string `json:",omitempty"`
		Name   string
		Offset string `json:",omitempty"`
	}{
		Name: col.Name,
	}

	if len(col.AST) > 0 {
		out.AST = sqlparser.String(sqlparser.Exprs(col.AST))
	}

	return json.Marshal(out)
}
