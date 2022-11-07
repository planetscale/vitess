package operators

func Merge(op1 []*View, op2 *View) ([]*View, error) {
	// Naive Merge
	return append(op1, op2), nil
}
