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

package planbuilder

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestCreateDB(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "main"}
	vschema := &vschemaWrapper{
		v: &vindexes.VSchema{
			Keyspaces: map[string]*vindexes.KeyspaceSchema{"main": {Keyspace: ks}},
		},
		keyspace: ks,
	}

	// default behaviour
	plan, err := TestBuilder("create database test", vschema)
	require.NoError(t, err)

	_, err = plan.Instructions.Execute(nil, nil, true)
	require.EqualError(t, err, "create database not allowed")

	// we make sure to restore the state so we don't destabilize other tests
	before := databaseCreator
	defer func() {
		databaseCreator = before
	}()

	// setting custom behaviour for CREATE DATABASE
	databaseCreator = &fakePlugin{}

	plan, err = TestBuilder("create database test", vschema)
	require.NoError(t, err)
	_, err = plan.Instructions.Execute(nil, nil, true)
	require.NoError(t, err)
}

type fakePlugin struct{}

// CreateDatabase is a fake
func (*fakePlugin) CreateDatabase(ast *sqlparser.CreateDatabase) error {
	return nil
}

// DropDatabase is a fake
func (*fakePlugin) DropDatabase(ast *sqlparser.DropDatabase) error {
	return nil
}
