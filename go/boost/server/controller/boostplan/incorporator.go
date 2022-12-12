package boostplan

import (
	"fmt"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/boost/boostpb"
	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/boost/server/controller/boostplan/operators"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Incorporator struct {
	converter *operators.Converter
	leafs     map[string]*operators.Query
	reuseType boostpb.ReuseType
}

func NewIncorporator() *Incorporator {
	return &Incorporator{
		converter: operators.NewConverter(),
		leafs:     make(map[string]*operators.Query),
		reuseType: boostpb.ReuseType_FINKELSTEIN,
	}
}

func (inc *Incorporator) Clone() *Incorporator {
	return &Incorporator{
		converter: inc.converter.Clone(),
		leafs:     maps.Clone(inc.leafs),
		reuseType: inc.reuseType,
	}
}

func (inc *Incorporator) SetReuse(r boostpb.ReuseType) {
	inc.reuseType = r
}

func (inc *Incorporator) AddParsedQuery(keyspace string, stmt sqlparser.Statement, id string, mig Migration, si *SchemaInformation) (QFP, error) {
	if id == "" {
		return nil, fmt.Errorf("missing ID for parsed query")
	}

	switch stmt.(type) {
	case sqlparser.SelectStatement:
		return inc.addSelectQuery(keyspace, id, stmt, mig, si)
	default:
		return nil, &UnsupportedQueryTypeError{Query: stmt}
	}
}

func (inc *Incorporator) addSelectQuery(keyspace, id string, sel sqlparser.Statement, mig Migration, si *SchemaInformation) (*operators.QueryFlowParts, error) {
	view, tr, err := inc.converter.Plan(si.Schema, si.Semantics(keyspace), sel, keyspace, id)
	if err != nil {
		return nil, err
	}

	q := &operators.Query{
		PublicID: id,
		Roots:    view.Roots(),
		View:     view,
	}

	qfp, err := queryToFlowParts(mig, tr, q)
	if err != nil {
		return nil, err
	}

	inc.leafs[id] = q
	return qfp, nil
}

func (inc *Incorporator) UpgradeSchema(newVersion int64) {
	inc.converter.UpgradeSchema(newVersion)
}

func (inc *Incorporator) RemoveQuery(id string) graph.NodeIdx {
	query, ok := inc.leafs[id]
	if !ok {
		return graph.InvalidNode
	}
	delete(inc.leafs, id)
	inc.converter.RemoveQueryByPublicID(id)

	for _, nid := range inc.leafs {
		if nid.Leaf() == query.Leaf() {
			// More than one query uses this leaf! Don't remove it yet
			return graph.InvalidNode
		}
	}
	return query.Leaf()
}

func (inc *Incorporator) EnableReuse(reuse boostpb.ReuseType) {
	inc.reuseType = reuse
}

func (inc *Incorporator) IsLeafAddress(ni graph.NodeIdx) bool {
	for _, nn := range inc.leafs {
		if nn.Leaf() == ni {
			return true
		}
	}
	return false
}
