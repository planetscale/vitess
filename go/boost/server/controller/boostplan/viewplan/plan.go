package viewplan

func (vp *Plan) CanMaterializePartially() bool {
	return vp.TreeKey == nil
}
