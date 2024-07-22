/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Recurse is used to represent recursive CTEs
// Init is used to represent the initial query.
// It's result are then used to start the recursion on the Recurse side
// The values being sent to the Recurse side are stored in the Vars map -
// the key is the bindvar name and the value is the index of the column in the recursive result
type Recurse struct {
	Init, Recurse Primitive

	Vars map[string]int
}

var _ Primitive = (*Recurse)(nil)

func (r *Recurse) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := vcursor.ExecutePrimitive(ctx, r.Init, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// recurseRows contains the rows used in the next recursion
	recurseRows := res.Rows
	joinVars := make(map[string]*querypb.BindVariable)
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		for _, row := range theseRows {
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			rresult, err := vcursor.ExecutePrimitive(ctx, r.Recurse, combineVars(bindVars, joinVars), false)
			if err != nil {
				return nil, err
			}
			recurseRows = append(recurseRows, rresult.Rows...)
			res.Rows = append(res.Rows, rresult.Rows...)
		}
	}
	return res, nil
}

func (r *Recurse) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (r *Recurse) RouteType() string {
	return "Recurse"
}

func (r *Recurse) GetKeyspaceName() string {
	if r.Init.GetKeyspaceName() == r.Recurse.GetKeyspaceName() {
		return r.Init.GetKeyspaceName()
	}
	return r.Init.GetKeyspaceName() + "_" + r.Recurse.GetKeyspaceName()
}

func (r *Recurse) GetTableName() string {
	return r.Init.GetTableName()
}

func (r *Recurse) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Init.GetFields(ctx, vcursor, bindVars)
}

func (r *Recurse) NeedsTransaction() bool {
	return false
}

func (r *Recurse) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{r.Init, r.Recurse}, nil
}

func (r *Recurse) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Recurse",
	}
}
