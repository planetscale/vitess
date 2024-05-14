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

type Recurse struct {
	// The Init side is what starts the recursion
	Init, Iter Primitive

	// Vars is the list of variables that are passed from each iteration to the next
	// The key is the name of the variable, and the value is the index in the row
	Vars map[string]int `json:",omitempty"`
}

func (r *Recurse) RouteType() string {
	return "Recurse"
}

func (r *Recurse) GetKeyspaceName() string {
	return formatTwoOptionsNicely(
		r.Init.GetKeyspaceName(),
		r.Iter.GetKeyspaceName(),
	)
}

func (r *Recurse) GetTableName() string {
	return formatTwoOptionsNicely(
		r.Init.GetTableName(),
		r.Iter.GetTableName(),
	)
}

func (r *Recurse) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Init.GetFields(ctx, vcursor, bindVars)
}

func (r *Recurse) NeedsTransaction() bool {
	return r.Init.NeedsTransaction() || r.Iter.NeedsTransaction()
}

func (r *Recurse) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var current []sqltypes.Row
	var results []sqltypes.Row
	init, err := r.Init.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	current = init.Rows
	results = append(results, init.Rows...)
	joinVars := make(map[string]*querypb.BindVariable)
	for len(current) > 0 {
		var next []sqltypes.Row
		for _, row := range current {
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			rresult, err := vcursor.ExecutePrimitive(ctx, r.Iter, combineVars(bindVars, joinVars), wantfields)
			if err != nil {
				return nil, err
			}
			next = append(next, rresult.Rows...)
		}
		results = append(results, next...)
		current = next
	}

	return &sqltypes.Result{
		Rows: results,
	}, nil
}

func (r *Recurse) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// TODO: stream that shit
	res, err := r.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

func (r *Recurse) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{r.Init, r.Iter}, nil
}

func (r *Recurse) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Recurse",
	}
}
