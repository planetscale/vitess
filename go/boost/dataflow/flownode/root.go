package flownode

var _ NodeImpl = (*Root)(nil)

type Root struct{}

func (n *Root) dataflow() {}
