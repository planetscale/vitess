package operators

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/tools/graphviz"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestNodeReuse(t *testing.T) {
	/* The tree that these tests is working on looks like this
	           ┌─┐
	           │0│
	           └┬┘
	      ┌─┐   │   ┌─┐
	      │1├───┴───┤2│
	      └┬┘       └┬┘
	   ┌─┐ │ ┌─┐ ┌─┐ │ ┌─┐
	   │3├─┴─┤4│ │5├─┴─┤6│
	   └─┘   └─┘ └─┘   └─┘
	*/

	t.Run("nothing can be reused", func(t *testing.T) {
		tester := createTree()
		tester.test()
		assert.Empty(t, tester.reusedNodes())
	})

	t.Run("two leaf nodes can be shared", func(t *testing.T) {
		tester := createTree()
		tester.nodesAreEqual(3, 5)
		tester.test()

		assert.Equal(t, tester.nodes[1].Ancestors[0], tester.nodes[2].Ancestors[0])
		assert.Equal(t, []*Node{tester.nodes[3]}, tester.reusedNodes())
	})

	t.Run("two sub-trees can be shared", func(t *testing.T) {
		tester := createTree()
		tester.nodesAreEqual(1, 2)
		tester.nodesAreEqual(3, 5)
		tester.nodesAreEqual(4, 6)
		tester.test()

		assert.Equal(t, tester.nodes[0].Ancestors[0], tester.nodes[0].Ancestors[1])
		assert.Equal(t, []*Node{tester.nodes[1], tester.nodes[3], tester.nodes[4]}, tester.reusedNodes())
	})

	t.Run("sub trees cant be shared because not all leaves are equal ", func(t *testing.T) {
		tester := createTree()
		tester.nodesAreEqual(1, 2)
		tester.nodesAreEqual(3, 5)
		tester.test()

		assert.Equal(t, []*Node{tester.nodes[3]}, tester.reusedNodes())
	})
}

type tester struct {
	nodes []*Node
	ops   []*fakeOp
}

func (t *tester) nodesAreEqual(a, b int) {
	aNode := t.ops[a]
	bNode := t.ops[b]
	if aNode.equalTo == nil {
		aNode.equalTo = map[Operator]any{}
	}
	if bNode.equalTo == nil {
		bNode.equalTo = map[Operator]any{}
	}

	aNode.equalTo[bNode] = nil
	bNode.equalTo[aNode] = nil
}

func (t *tester) assertNodesAre() *Node {
	return t.nodes[0]
}

func (t *tester) test() {
	st := semantics.EmptySemTable()
	r := NodeReuser{cache: map[Hash][]*Node{}}
	r.Visit(st, t.nodes[0])
}

func (t *tester) reusedNodes() (result []*Node) {
	queue := []*Node{t.nodes[0]}
	seen := map[*Node]any{}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], this.Ancestors...)
		if _, ok := seen[this]; ok {
			result = append(result, this)
		} else {
			seen[this] = nil
		}
	}
	return
}

func createTree() *tester {
	/* This method creates a tree that looks like this
	           ┌─┐
	           │0│
	           └┬┘
	      ┌─┐   │   ┌─┐
	      │1├───┴───┤2│
	      └┬┘       └┬┘
	   ┌─┐ │ ┌─┐ ┌─┐ │ ┌─┐
	   │3├─┴─┤4│ │5├─┴─┤6│
	   └─┘   └─┘ └─┘   └─┘

	*/
	var ops []*fakeOp
	var nodes []*Node
	for i := 0; i <= 6; i++ {
		op := &fakeOp{}
		ops = append(ops, op)
		nodes = append(nodes, &Node{Name: fmt.Sprintf("%d", i), Op: op})
	}
	nodes[0].Ancestors = []*Node{nodes[1], nodes[2]}
	nodes[1].Ancestors = []*Node{nodes[3], nodes[4]}
	nodes[2].Ancestors = []*Node{nodes[5], nodes[6]}

	return &tester{
		nodes: nodes,
		ops:   ops,
	}
}

var _ Operator = (*fakeOp)(nil)

type fakeOp struct {
	equalTo map[Operator]any
	id      semantics.TableSet
}

func (f *fakeOp) Signature() QuerySignature {
	return QuerySignature{}
}

func (f *fakeOp) addToGraph(g *graphviz.Graph) (*graphviz.Node, error) {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) AddColumns(ctx *PlanContext, col Columns) (Columns, error) {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) IntroducesTableID() *semantics.TableSet {
	return &f.id
}

func (f *fakeOp) KeepAncestorColumns() bool {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) PlanOffsets(node *Node, st *semantics.SemTable) error {
	// TODO implement me
	panic("implement me")
}

func (f *fakeOp) Equals(st *semantics.SemTable, op Operator) bool {
	if f.equalTo == nil {
		return false
	}
	_, found := f.equalTo[op]
	return found
}
