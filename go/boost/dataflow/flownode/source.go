package flownode

var _ NodeImpl = (*Source)(nil)

type Source struct{}

func (n *Source) dataflow() {}
