/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type ResolveDestination struct {
	Left, Right Primitive
}

func (r *ResolveDestination) RouteType() string {
	return "resolve destination"
}

func (r *ResolveDestination) GetKeyspaceName() string {
	return r.Right.GetKeyspaceName()
}

func (r *ResolveDestination) GetTableName() string {
	return r.Right.GetTableName()
}

func (r *ResolveDestination) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (r *ResolveDestination) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error, targets ...*DestinationInformation) error {
	r.Left.StreamExecute(vcursor, bindVars, false, func(result *sqltypes.Result) error {
		// resolve shards
		// TODO: shard = results
		r.Right.StreamExecute(vcursor, bindVars, wantfields, func(res *sqltypes.Result) error {
			return nil
		}, shard)
		return nil
	})
	return nil
}

func (r *ResolveDestination) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (r *ResolveDestination) NeedsTransaction() bool {
	return r.Right.NeedsTransaction()
}

func (r *ResolveDestination) Inputs() []Primitive {
	return []Primitive{r.Left, r.Right}
}

func (r *ResolveDestination) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Resolve Destination",
	}
}

var _ Primitive = (*ResolveDestination)(nil)
