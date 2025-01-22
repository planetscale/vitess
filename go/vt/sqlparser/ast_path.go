package sqlparser

type (
	ASTStep int

	ASTPath []ASTStep

	pathCursor struct {
		path ASTPath
	}
)

const (
	ComparisonExprLeft ASTStep = iota
	ComparisonExprRight
	ComparisonExprEscape
	ColNameName
	ColNameQualifier
	BinaryExprLeft
	BinaryExprRight
)

func WalkWithPath(node SQLNode, fn func(node SQLNode, path ASTPath) error) error {
	pc := &pathCursor{}
	return pc.walkWithPathSQLNode(node, fn)
}

func GetNodeByPath(node SQLNode, path ASTPath) SQLNode {
	if len(path) == 0 {
		return node
	}
	switch path[0] {
	case ComparisonExprLeft:
		if node, ok := node.(*ComparisonExpr); ok {
			return GetNodeByPath(node.Left, path[1:])
		}
	case ComparisonExprRight:
		if node, ok := node.(*ComparisonExpr); ok {
			return GetNodeByPath(node.Right, path[1:])
		}
	case ComparisonExprEscape:
		if node, ok := node.(*ComparisonExpr); ok {
			return GetNodeByPath(node.Escape, path[1:])
		}
	case ColNameName:
		if node, ok := node.(*ColName); ok {
			return GetNodeByPath(node.Name, path[1:])
		}
	case ColNameQualifier:
		if node, ok := node.(*ColName); ok {
			return GetNodeByPath(node.Qualifier, path[1:])
		}
	case BinaryExprLeft:
		if node, ok := node.(*BinaryExpr); ok {
			return GetNodeByPath(node.Left, path[1:])
		}
	case BinaryExprRight:
		if node, ok := node.(*BinaryExpr); ok {
			return GetNodeByPath(node.Right, path[1:])
		}
	}
	return nil
}

func (pc *pathCursor) walkWithPathSQLNode(node SQLNode, fn func(node SQLNode, path ASTPath) error) error {
	if node == nil {
		return nil
	}
	switch node := node.(type) {
	case *ComparisonExpr:
		return pc.walkWithPathComparisonExpr(node, fn)
	case *ColName:
		return pc.walkWithPathColName(node, fn)
	case *BinaryExpr:
		return pc.walkWithPathBinaryExpr(node, fn)

	}
	return nil
}

func (pc *pathCursor) walkWithPathComparisonExpr(node *ComparisonExpr, fn func(node SQLNode, path ASTPath) error) error {
	if err := fn(node, pc.path); err != nil {
		return err
	}
	pc.path = append(pc.path, ComparisonExprLeft)
	if err := pc.walkWithPathSQLNode(node.Left, fn); err != nil {
		return err
	}
	pc.popLastStep()

	pc.path = append(pc.path, ComparisonExprRight)
	if err := pc.walkWithPathSQLNode(node.Right, fn); err != nil {
		return err
	}
	pc.popLastStep()

	pc.path = append(pc.path, ComparisonExprEscape)
	if err := pc.walkWithPathSQLNode(node.Escape, fn); err != nil {
		return err
	}
	pc.popLastStep()

	return nil

}

func (pc *pathCursor) popLastStep() {
	pc.path = pc.path[:len(pc.path)-1]
}

func (pc *pathCursor) walkWithPathColName(node *ColName, fn func(node SQLNode, path ASTPath) error) error {
	if err := fn(node, pc.path); err != nil {
		return err
	}
	pc.path = append(pc.path, ColNameName)
	if err := pc.walkWithPathSQLNode(node.Name, fn); err != nil {
		return err
	}
	pc.popLastStep()

	pc.path = append(pc.path, ColNameQualifier)
	if err := pc.walkWithPathSQLNode(node.Qualifier, fn); err != nil {
		return err
	}
	pc.popLastStep()

	return nil
}

func (pc *pathCursor) walkWithPathBinaryExpr(node *BinaryExpr, fn func(node SQLNode, path ASTPath) error) error {
	if err := fn(node, pc.path); err != nil {
		return err
	}
	pc.path = append(pc.path, BinaryExprLeft)
	if err := pc.walkWithPathSQLNode(node.Left, fn); err != nil {
		return err
	}
	pc.popLastStep()

	pc.path = append(pc.path, BinaryExprRight)
	if err := pc.walkWithPathSQLNode(node.Right, fn); err != nil {
		return err
	}
	pc.popLastStep()

	return nil
}
