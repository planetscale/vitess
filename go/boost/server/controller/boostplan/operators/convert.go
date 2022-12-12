package operators

import (
	"fmt"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/boost/graph"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	Converter struct {
		current map[string]int64
		nodes   map[noderef]*Node
		version int64
		count   int
	}
	noderef struct {
		name    string
		version int64
	}
)

func NewConverter() *Converter {
	return &Converter{
		current: make(map[string]int64),
		nodes:   make(map[noderef]*Node),
		version: 0,
	}
}

func (conv *Converter) Clone() *Converter {
	return &Converter{
		current: maps.Clone(conv.current),
		nodes:   maps.Clone(conv.nodes),
		version: conv.version,
		count:   conv.count,
	}
}

func (conv *Converter) NewNode(name string, op Operator, inputs []*Node) *Node {
	// always uniquefy all node names
	name = fmt.Sprintf("%s%d", name, conv.count)

	n := &Node{
		Name:      name,
		Version:   conv.version,
		Ancestors: inputs,
		Op:        op,
		Flow:      FlowNode{Address: graph.InvalidNode},
	}

	ch, ok := op.(ColumnHolder)
	if ok {
		n.Columns = ch.GetColumns()
	}

	nodeID := noderef{name, conv.version}
	if _, ok := conv.nodes[nodeID]; !ok {
		conv.current[name] = conv.version
		conv.nodes[nodeID] = n
	}

	conv.count++
	return n
}

func (conv *Converter) loadNamedTable(ddl DDLSchema, keyspace, name string) (*Node, error) {
	tableSpec, err := ddl.LoadTableSpec(keyspace, name)
	if err != nil {
		return nil, err
	}

	n, err := conv.makeTableNode(keyspace, name, tableSpec)
	if err != nil {
		return nil, err
	}

	tableName := externalTableName(keyspace, name)
	nodeID := noderef{tableName, conv.version}
	if _, ok := conv.nodes[nodeID]; !ok {
		conv.current[tableName] = conv.version
		conv.nodes[nodeID] = n
	}
	return n, nil
}

func indexContainsNullableColumn(index *sqlparser.IndexDefinition, spec *sqlparser.TableSpec) bool {
	for _, indexCol := range index.Columns {
		for _, tableCol := range spec.Columns {
			if indexCol.Column.String() == tableCol.Name.String() {
				if tableCol.Type.Options == nil || tableCol.Type.Options.Null == nil || *tableCol.Type.Options.Null {
					return true
				}
			}
		}
	}
	return false
}

func (conv *Converter) makeTableNode(keyspace, name string, spec *sqlparser.TableSpec) (*Node, error) {
	var primaryKey *sqlparser.IndexDefinition

	for _, idx := range spec.Indexes {
		if idx.Info.Primary {
			primaryKey = idx
			break
		}
	}

	// If we have no primary key, we fall back to the first unique non-nullable key
	// which can function as the primary key for Boost. This is a requirement we already
	// enforce for online DDL as well, so it should always pass for any production
	// branch under normal circumstances.
	if primaryKey == nil {
		for _, idx := range spec.Indexes {
			if idx.Info.Unique && !indexContainsNullableColumn(idx, spec) {
				primaryKey = idx
				break
			}
		}
	}

	var colspecs []ColumnSpec

	for _, col := range spec.Columns {
		colspecs = append(colspecs, ColumnSpec{Column: col})
	}

	if primaryKey != nil {
		var columnKeys []Column
		for _, col := range primaryKey.Columns {
			columnKeys = append(columnKeys, Column{Name: col.Column.String()})
		}
		op := &Table{
			Keyspace:    keyspace,
			TableName:   name,
			VColumns:    nil,
			ColumnSpecs: colspecs,
			Keys:        columnKeys,
			Spec:        spec,
		}
		return conv.NewNode(name, op, nil), nil
	}

	return nil, &NoUniqueKeyError{
		Keyspace: keyspace,
		Table:    name,
		Spec:     spec,
	}
}

func (conv *Converter) UpgradeSchema(newversion int64) {
	if newversion <= conv.version {
		panic("schema version is not newer than the existing")
	}
	conv.version = newversion
}

func (conv *Converter) findViewByPublicID(id string) (noderef, *Node, bool) {
	for nref, nn := range conv.nodes {
		switch op := nn.Op.(type) {
		case *View:
			if op.PublicID == id {
				return nref, nn, true
			}
		}
	}
	return noderef{}, nil, false
}

func (conv *Converter) RemoveQueryByPublicID(id string) {
	nref, leaf, ok := conv.findViewByPublicID(id)
	if !ok {
		panic("tried to remove unknown query")
	}

	delete(conv.current, nref.name)
	delete(conv.nodes, nref)

	var deque []*Node
	deque = append(deque, leaf)

	for len(deque) > 0 {
		n := deque[0]
		deque = deque[1:]
		deque = append(deque, n.Ancestors...)

		if _, isbase := n.Op.(*Table); isbase {
			continue
		}
		if len(n.Children) > 0 {
			continue
		}
		delete(conv.nodes, noderef{n.Name, n.Version})
	}
}

func newTableRef(st *semantics.SemTable, tableNode *Node, v int64, id semantics.TableSet, hints sqlparser.IndexHints) (*Node, error) {
	op := &NodeTableRef{TableID: id, Node: tableNode, Hints: hints}
	n := &Node{
		Name:    "external_base",
		Version: v,
		Op:      op,
		Flow:    FlowNode{Address: graph.InvalidNode},
	}
	if ch, ok := tableNode.Op.(ColumnHolder); ok {
		for _, column := range ch.GetColumns() {
			ast, err := column.SingleAST()
			if err != nil {
				return nil, NewBug("table specs should not have multiple AST")
			}
			st.Direct[ast] = id
			n.Columns = append(n.Columns, &Column{
				AST:  []sqlparser.Expr{ast},
				Name: column.Name,
			})
		}
		op.Columns = n.Columns
	}
	return n, nil
}
