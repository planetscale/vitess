// Code generated by optgen; DO NOT EDIT.

package aggregation

import (
	"vitess.io/vitess/go/test/go-mysql-server/sql"
	"vitess.io/vitess/go/test/go-mysql-server/sql/expression"
)

type RowsUnboundedPrecedingToNPrecedingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsUnboundedPrecedingToNPrecedingFramer)(nil)

func NewRowsUnboundedPrecedingToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	endNPreceding, err := expression.LiteralToInt(frame.EndNPreceding())
	if err != nil {
		return nil, err
	}
	return &RowsUnboundedPrecedingToNPrecedingFramer{
		rowFramerBase{
			unboundedPreceding: unboundedPreceding,
			endNPreceding:      endNPreceding,
		},
	}, nil
}

type RowsUnboundedPrecedingToCurrentRowFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsUnboundedPrecedingToCurrentRowFramer)(nil)

func NewRowsUnboundedPrecedingToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	endCurrentRow := true
	return &RowsUnboundedPrecedingToCurrentRowFramer{
		rowFramerBase{
			unboundedPreceding: unboundedPreceding,
			endCurrentRow:      endCurrentRow,
		},
	}, nil
}

type RowsUnboundedPrecedingToNFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsUnboundedPrecedingToNFollowingFramer)(nil)

func NewRowsUnboundedPrecedingToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	endNFollowing, err := expression.LiteralToInt(frame.EndNFollowing())
	if err != nil {
		return nil, err
	}
	return &RowsUnboundedPrecedingToNFollowingFramer{
		rowFramerBase{
			unboundedPreceding: unboundedPreceding,
			endNFollowing:      endNFollowing,
		},
	}, nil
}

type RowsUnboundedPrecedingToUnboundedFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsUnboundedPrecedingToUnboundedFollowingFramer)(nil)

func NewRowsUnboundedPrecedingToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	unboundedFollowing := true
	return &RowsUnboundedPrecedingToUnboundedFollowingFramer{
		rowFramerBase{
			unboundedPreceding: unboundedPreceding,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RowsNPrecedingToNPrecedingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNPrecedingToNPrecedingFramer)(nil)

func NewRowsNPrecedingToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding, err := expression.LiteralToInt(frame.StartNPreceding())
	if err != nil {
		return nil, err
	}
	endNPreceding, err := expression.LiteralToInt(frame.EndNPreceding())
	if err != nil {
		return nil, err
	}
	return &RowsNPrecedingToNPrecedingFramer{
		rowFramerBase{
			startNPreceding: startNPreceding,
			endNPreceding:   endNPreceding,
		},
	}, nil
}

type RowsNPrecedingToCurrentRowFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNPrecedingToCurrentRowFramer)(nil)

func NewRowsNPrecedingToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding, err := expression.LiteralToInt(frame.StartNPreceding())
	if err != nil {
		return nil, err
	}
	endCurrentRow := true
	return &RowsNPrecedingToCurrentRowFramer{
		rowFramerBase{
			startNPreceding: startNPreceding,
			endCurrentRow:   endCurrentRow,
		},
	}, nil
}

type RowsNPrecedingToNFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNPrecedingToNFollowingFramer)(nil)

func NewRowsNPrecedingToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding, err := expression.LiteralToInt(frame.StartNPreceding())
	if err != nil {
		return nil, err
	}
	endNFollowing, err := expression.LiteralToInt(frame.EndNFollowing())
	if err != nil {
		return nil, err
	}
	return &RowsNPrecedingToNFollowingFramer{
		rowFramerBase{
			startNPreceding: startNPreceding,
			endNFollowing:   endNFollowing,
		},
	}, nil
}

type RowsNPrecedingToUnboundedFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNPrecedingToUnboundedFollowingFramer)(nil)

func NewRowsNPrecedingToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding, err := expression.LiteralToInt(frame.StartNPreceding())
	if err != nil {
		return nil, err
	}
	unboundedFollowing := true
	return &RowsNPrecedingToUnboundedFollowingFramer{
		rowFramerBase{
			startNPreceding:    startNPreceding,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RowsCurrentRowToNPrecedingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsCurrentRowToNPrecedingFramer)(nil)

func NewRowsCurrentRowToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	endNPreceding, err := expression.LiteralToInt(frame.EndNPreceding())
	if err != nil {
		return nil, err
	}
	return &RowsCurrentRowToNPrecedingFramer{
		rowFramerBase{
			startCurrentRow: startCurrentRow,
			endNPreceding:   endNPreceding,
		},
	}, nil
}

type RowsCurrentRowToCurrentRowFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsCurrentRowToCurrentRowFramer)(nil)

func NewRowsCurrentRowToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	endCurrentRow := true
	return &RowsCurrentRowToCurrentRowFramer{
		rowFramerBase{
			startCurrentRow: startCurrentRow,
			endCurrentRow:   endCurrentRow,
		},
	}, nil
}

type RowsCurrentRowToNFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsCurrentRowToNFollowingFramer)(nil)

func NewRowsCurrentRowToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	endNFollowing, err := expression.LiteralToInt(frame.EndNFollowing())
	if err != nil {
		return nil, err
	}
	return &RowsCurrentRowToNFollowingFramer{
		rowFramerBase{
			startCurrentRow: startCurrentRow,
			endNFollowing:   endNFollowing,
		},
	}, nil
}

type RowsCurrentRowToUnboundedFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsCurrentRowToUnboundedFollowingFramer)(nil)

func NewRowsCurrentRowToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	unboundedFollowing := true
	return &RowsCurrentRowToUnboundedFollowingFramer{
		rowFramerBase{
			startCurrentRow:    startCurrentRow,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RowsNFollowingToNPrecedingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNFollowingToNPrecedingFramer)(nil)

func NewRowsNFollowingToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing, err := expression.LiteralToInt(frame.StartNFollowing())
	if err != nil {
		return nil, err
	}
	endNPreceding, err := expression.LiteralToInt(frame.EndNPreceding())
	if err != nil {
		return nil, err
	}
	return &RowsNFollowingToNPrecedingFramer{
		rowFramerBase{
			startNFollowing: startNFollowing,
			endNPreceding:   endNPreceding,
		},
	}, nil
}

type RowsNFollowingToCurrentRowFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNFollowingToCurrentRowFramer)(nil)

func NewRowsNFollowingToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing, err := expression.LiteralToInt(frame.StartNFollowing())
	if err != nil {
		return nil, err
	}
	endCurrentRow := true
	return &RowsNFollowingToCurrentRowFramer{
		rowFramerBase{
			startNFollowing: startNFollowing,
			endCurrentRow:   endCurrentRow,
		},
	}, nil
}

type RowsNFollowingToNFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNFollowingToNFollowingFramer)(nil)

func NewRowsNFollowingToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing, err := expression.LiteralToInt(frame.StartNFollowing())
	if err != nil {
		return nil, err
	}
	endNFollowing, err := expression.LiteralToInt(frame.EndNFollowing())
	if err != nil {
		return nil, err
	}
	return &RowsNFollowingToNFollowingFramer{
		rowFramerBase{
			startNFollowing: startNFollowing,
			endNFollowing:   endNFollowing,
		},
	}, nil
}

type RowsNFollowingToUnboundedFollowingFramer struct {
	rowFramerBase
}

var _ sql.WindowFramer = (*RowsNFollowingToUnboundedFollowingFramer)(nil)

func NewRowsNFollowingToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing, err := expression.LiteralToInt(frame.StartNFollowing())
	if err != nil {
		return nil, err
	}
	unboundedFollowing := true
	return &RowsNFollowingToUnboundedFollowingFramer{
		rowFramerBase{
			startNFollowing:    startNFollowing,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RangeUnboundedPrecedingToNPrecedingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeUnboundedPrecedingToNPrecedingFramer)(nil)

func NewRangeUnboundedPrecedingToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	endNPreceding := frame.EndNPreceding()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeUnboundedPrecedingToNPrecedingFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			unboundedPreceding: unboundedPreceding,
			endNPreceding:      endNPreceding,
		},
	}, nil
}

type RangeUnboundedPrecedingToCurrentRowFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeUnboundedPrecedingToCurrentRowFramer)(nil)

func NewRangeUnboundedPrecedingToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	endCurrentRow := true
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeUnboundedPrecedingToCurrentRowFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			unboundedPreceding: unboundedPreceding,
			endCurrentRow:      endCurrentRow,
		},
	}, nil
}

type RangeUnboundedPrecedingToNFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeUnboundedPrecedingToNFollowingFramer)(nil)

func NewRangeUnboundedPrecedingToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	endNFollowing := frame.EndNFollowing()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeUnboundedPrecedingToNFollowingFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			unboundedPreceding: unboundedPreceding,
			endNFollowing:      endNFollowing,
		},
	}, nil
}

type RangeUnboundedPrecedingToUnboundedFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeUnboundedPrecedingToUnboundedFollowingFramer)(nil)

func NewRangeUnboundedPrecedingToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	unboundedPreceding := true
	unboundedFollowing := true
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeUnboundedPrecedingToUnboundedFollowingFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			unboundedPreceding: unboundedPreceding,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RangeNPrecedingToNPrecedingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNPrecedingToNPrecedingFramer)(nil)

func NewRangeNPrecedingToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding := frame.StartNPreceding()
	endNPreceding := frame.EndNPreceding()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNPrecedingToNPrecedingFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startNPreceding: startNPreceding,
			endNPreceding:   endNPreceding,
		},
	}, nil
}

type RangeNPrecedingToCurrentRowFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNPrecedingToCurrentRowFramer)(nil)

func NewRangeNPrecedingToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding := frame.StartNPreceding()
	endCurrentRow := true
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNPrecedingToCurrentRowFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startNPreceding: startNPreceding,
			endCurrentRow:   endCurrentRow,
		},
	}, nil
}

type RangeNPrecedingToNFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNPrecedingToNFollowingFramer)(nil)

func NewRangeNPrecedingToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding := frame.StartNPreceding()
	endNFollowing := frame.EndNFollowing()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNPrecedingToNFollowingFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startNPreceding: startNPreceding,
			endNFollowing:   endNFollowing,
		},
	}, nil
}

type RangeNPrecedingToUnboundedFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNPrecedingToUnboundedFollowingFramer)(nil)

func NewRangeNPrecedingToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNPreceding := frame.StartNPreceding()
	unboundedFollowing := true
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNPrecedingToUnboundedFollowingFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			startNPreceding:    startNPreceding,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RangeCurrentRowToNPrecedingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeCurrentRowToNPrecedingFramer)(nil)

func NewRangeCurrentRowToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	endNPreceding := frame.EndNPreceding()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeCurrentRowToNPrecedingFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startCurrentRow: startCurrentRow,
			endNPreceding:   endNPreceding,
		},
	}, nil
}

type RangeCurrentRowToCurrentRowFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeCurrentRowToCurrentRowFramer)(nil)

func NewRangeCurrentRowToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	endCurrentRow := true
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeCurrentRowToCurrentRowFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startCurrentRow: startCurrentRow,
			endCurrentRow:   endCurrentRow,
		},
	}, nil
}

type RangeCurrentRowToNFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeCurrentRowToNFollowingFramer)(nil)

func NewRangeCurrentRowToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	endNFollowing := frame.EndNFollowing()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeCurrentRowToNFollowingFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startCurrentRow: startCurrentRow,
			endNFollowing:   endNFollowing,
		},
	}, nil
}

type RangeCurrentRowToUnboundedFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeCurrentRowToUnboundedFollowingFramer)(nil)

func NewRangeCurrentRowToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startCurrentRow := true
	unboundedFollowing := true
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeCurrentRowToUnboundedFollowingFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			startCurrentRow:    startCurrentRow,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}

type RangeNFollowingToNPrecedingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNFollowingToNPrecedingFramer)(nil)

func NewRangeNFollowingToNPrecedingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing := frame.StartNFollowing()
	endNPreceding := frame.EndNPreceding()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNFollowingToNPrecedingFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startNFollowing: startNFollowing,
			endNPreceding:   endNPreceding,
		},
	}, nil
}

type RangeNFollowingToCurrentRowFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNFollowingToCurrentRowFramer)(nil)

func NewRangeNFollowingToCurrentRowFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing := frame.StartNFollowing()
	endCurrentRow := true
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNFollowingToCurrentRowFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startNFollowing: startNFollowing,
			endCurrentRow:   endCurrentRow,
		},
	}, nil
}

type RangeNFollowingToNFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNFollowingToNFollowingFramer)(nil)

func NewRangeNFollowingToNFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing := frame.StartNFollowing()
	endNFollowing := frame.EndNFollowing()
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNFollowingToNFollowingFramer{
		rangeFramerBase{
			orderBy:         orderBy,
			startNFollowing: startNFollowing,
			endNFollowing:   endNFollowing,
		},
	}, nil
}

type RangeNFollowingToUnboundedFollowingFramer struct {
	rangeFramerBase
}

var _ sql.WindowFramer = (*RangeNFollowingToUnboundedFollowingFramer)(nil)

func NewRangeNFollowingToUnboundedFollowingFramer(frame sql.WindowFrame, window *sql.WindowDefinition) (sql.WindowFramer, error) {
	startNFollowing := frame.StartNFollowing()
	unboundedFollowing := true
	if len(window.OrderBy) != 1 {
		return nil, ErrRangeInvalidOrderBy.New(len(window.OrderBy.ToExpressions()))
	}
	var orderBy sql.Expression
	if len(window.OrderBy) > 0 {
		orderBy = window.OrderBy.ToExpressions()[0]
	}
	return &RangeNFollowingToUnboundedFollowingFramer{
		rangeFramerBase{
			orderBy:            orderBy,
			startNFollowing:    startNFollowing,
			unboundedFollowing: unboundedFollowing,
		},
	}, nil
}