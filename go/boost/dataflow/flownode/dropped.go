package flownode

var _ NodeImpl = (*Dropped)(nil)

type Dropped struct{}

func (d *Dropped) dataflow() {}
