package operators

import (
	"fmt"

	"vitess.io/vitess/go/tools/graphviz"
	"vitess.io/vitess/go/vt/sqlparser"
)

func GraphViz(o []*Node) (*graphviz.Graph, error) {
	g := graphviz.New()
	nodes := make(map[*Node]*graphviz.Node)

	// init the map with the views
	for _, node := range o {
		n, err := node.addToGraph(g)
		if err != nil {
			return nil, err
		}
		nodes[node] = n
	}

	var err error

	// this is the queue of nodes we still haven't seen the inputs to
	queue := append([]*Node{}, o...)

	for len(queue) > 0 {
		this := queue[0]
		queue = queue[1:]
		thisNode := nodes[this]
		for _, in := range this.Ancestors {
			inNode, isPresent := nodes[in]
			if !isPresent {
				// if we can't find the input in the map, we have yet to visit it and the children
				inNode, err = in.addToGraph(g)
				if err != nil {
					return nil, err
				}
				nodes[in] = inNode
				queue = append(queue, in)
			}
			g.AddEdge(inNode, thisNode)
		}
	}

	return g, nil
}

func (node *Node) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	graphNode, err := node.Op.addToGraph(g)
	if err != nil {
		return nil, err

	}
	var cols []string
	for _, column := range node.Columns {
		cols = append(cols, column.Name)
	}
	graphNode.AddSection("Columns", cols)
	return graphNode, nil
}

func (t *Table) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	node := g.AddNode("Table")
	node.AddAttribute(fmt.Sprintf("%s.%s", t.Keyspace, t.TableName))
	return node, nil
}

func (j *Join) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	this := g.AddNode("Join")
	if j.Inner {
		this.AddAttribute("inner")
	} else {
		this.AddAttribute("outer")
	}
	this.AddAttribute(sqlparser.String(j.Predicates))
	return this, nil
}

func (f *Filter) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	this := g.AddNode("Filter")
	this.AddSection("Predicate", []string{sqlparser.String(f.Predicates)})
	return this, nil
}

func (g *GroupBy) addToGraph(graph *graphviz.Graph) (*graphviz.Node, error) {
	this := graph.AddNode("HashGrouping")
	var groupingCol []string
	for _, column := range g.Grouping {
		groupingCol = append(groupingCol, column.Name)
	}
	if len(groupingCol) > 0 {
		this.AddSection("GroupBy", groupingCol)
	}
	var aggrCol []string
	for _, aggr := range g.Aggregations {
		aggrCol = append(aggrCol, sqlparser.String(aggr))
	}
	if len(aggrCol) > 0 {
		this.AddSection("Aggregate", aggrCol)
	}
	return this, nil
}

func (p *Project) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	this := g.AddNode("Project")
	for _, column := range p.Columns {
		name, details := column.Explain()
		if name == "" {
			this.AddAttribute(details)
		} else if details == "" {
			this.AddAttribute(name)
		} else {
			this.AddAttribute(fmt.Sprintf("%s AS %s", details, name))
		}
	}

	return this, nil
}

func (v *View) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	this := g.AddNode("View")
	for _, p := range v.Parameters {
		col := "key: " + p.key.Name
		if p.Name == "" {
			this.AddAttribute(col)
		} else {
			this.AddAttribute(fmt.Sprintf("%s AS :%s", col, p.Name))
		}
	}

	return this, nil
}

func (n *NodeTableRef) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	result, err := n.Node.addToGraph(g)
	if err != nil {
		return nil, err
	}
	result.AddAttribute("reused")
	return result, nil
}

func (u *Union) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	result := g.AddNode("Union")
	for _, col := range u.Columns {
		result.AddAttribute(col.Name)
	}
	return result, nil
}

func (t *TopK) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	result := g.AddNode("TopK")

	var order []string
	for _, o := range t.Order {
		order = append(order, sqlparser.String(o))
	}
	result.AddSection("ORDER BY", order)
	result.AddAttribute(fmt.Sprintf("K: %d", t.K))
	return result, nil
}

func (d *Distinct) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	return g.AddNode("distinct"), nil
}
