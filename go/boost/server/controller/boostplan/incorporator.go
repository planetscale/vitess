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
	leafs     map[string]graph.NodeIdx

	namedQueries map[string]operators.Hash
	operators    map[operators.Hash]*operators.Node
	numQueries   int

	reuseType boostpb.ReuseType
}

func NewIncorporator() *Incorporator {
	return &Incorporator{
		converter:    operators.NewConverter(),
		leafs:        make(map[string]graph.NodeIdx),
		namedQueries: make(map[string]operators.Hash),
		operators:    make(map[operators.Hash]*operators.Node),
		numQueries:   0,
		reuseType:    boostpb.ReuseType_FINKELSTEIN,
	}
}

func (inc *Incorporator) Clone() *Incorporator {
	return &Incorporator{
		converter:    inc.converter.Clone(),
		leafs:        maps.Clone(inc.leafs),
		namedQueries: maps.Clone(inc.namedQueries),
		operators:    maps.Clone(inc.operators),
		numQueries:   inc.numQueries,
		reuseType:    inc.reuseType,
	}
}

func (inc *Incorporator) SetReuse(r boostpb.ReuseType) {
	inc.reuseType = r
}

func (inc *Incorporator) AddParsedQuery(keyspace string, stmt sqlparser.Statement, name string, isLeaf bool, mig Migration, si *SchemaInformation) (QFP, error) {
	if name == "" {
		return inc.nodesForQuery(keyspace, stmt, isLeaf, mig, si)
	}
	return inc.nodesForNamedQuery(keyspace, stmt, name, isLeaf, mig, si)
}

func (inc *Incorporator) nodesForQuery(keyspace string, stmt sqlparser.Statement, isLeaf bool, mig Migration, si *SchemaInformation) (QFP, error) {
	var name string

	switch stmt := stmt.(type) {
	case sqlparser.SelectStatement:
		name = fmt.Sprintf("q_%d", inc.numQueries)
	default:
		return nil, &UnsupportedQueryTypeError{Query: stmt}
	}

	return inc.nodesForNamedQuery(keyspace, stmt, name, isLeaf, mig, si)
}

func (inc *Incorporator) addSelectQuery(keyspace, name string, sel sqlparser.Statement, mig Migration, si *SchemaInformation) (*operators.QueryFlowParts, error) {
	_, op, tr, err := inc.converter.Plan(si.Schema, si.semantics(keyspace), sel, keyspace, name)
	if err != nil {
		return nil, err
	}

	return inc.addQuery(name, op, mig, tr)
}

func (inc *Incorporator) addQuery(name string, view *operators.Node, mig Migration, tr *operators.TableReport) (*operators.QueryFlowParts, error) {
	roots := view.Roots()
	q := &operators.Query{
		Name:  name,
		Roots: roots,
		View:  view,
	}
	qfp, err := OpQueryToFlowParts(q, tr, mig)
	if err != nil {
		return nil, err
	}
	inc.registerQuery(name, view)
	return qfp, nil
}

func (inc *Incorporator) registerQuery(name string, view *operators.Node) {
	if view == nil {
		panic("registerQuery without a View")
	}

	hash := view.Signature().Hash()
	inc.operators[hash] = view
	inc.namedQueries[name] = hash
}

func (inc *Incorporator) nodesForNamedQuery(keyspace string, stmt sqlparser.Statement, name string, isLeaf bool, mig Migration, si *SchemaInformation) (QFP, error) {
	qfp, err := inc.addStatement(keyspace, stmt, name, mig, si)
	if err != nil {
		return nil, err
	}

	inc.leafs[name] = qfp.QueryLeaf
	return qfp, nil
}

func (inc *Incorporator) addStatement(keyspace string, stmt sqlparser.Statement, name string, mig Migration, si *SchemaInformation) (*operators.QueryFlowParts, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return inc.addSelectQuery(keyspace, name, stmt, mig, si)
	}
	return nil, fmt.Errorf("unhandled query type in recipe: %T", stmt)
}

func (inc *Incorporator) UpgradeSchema(newVersion int64) {
	inc.converter.UpgradeSchema(newVersion)
}

func (inc *Incorporator) RemoveQuery(name string) graph.NodeIdx {
	nodeID, ok := inc.leafs[name]
	if !ok {
		return graph.InvalidNode
	}
	delete(inc.leafs, name)

	delete(inc.namedQueries, name)
	inc.converter.RemoveQuery(name)

	for _, nid := range inc.leafs {
		if nid == nodeID {
			// More than one query uses this leaf! Don't remove it yet
			return graph.InvalidNode
		}
	}
	return nodeID
}

func (inc *Incorporator) EnableReuse(reuse boostpb.ReuseType) {
	inc.reuseType = reuse
}

func (inc *Incorporator) GetQueryAddress(name string) (graph.NodeIdx, bool) {
	if na, ok := inc.leafs[name]; ok {
		return na, true
	}

	return inc.converter.GetView(name)
}

func (inc *Incorporator) IsLeafAddress(ni graph.NodeIdx) bool {
	for _, nn := range inc.leafs {
		if nn == ni {
			return true
		}
	}
	return false
}
