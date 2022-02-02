/*
Copyright 2022 The Vitess Authors.

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

package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Horizon struct {
	Source    LogicalOperator
	Statement sqlparser.SelectStatement
}

var _ LogicalOperator = (*Horizon)(nil)

// iLogical implements the LogicalOperator interface
func (p *Horizon) iLogical() {}

// TableID implements the LogicalOperator interface
func (p *Horizon) TableID() semantics.TableSet {
	return p.Source.TableID()
}

// UnsolvedPredicates implements the LogicalOperator interface
func (p *Horizon) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return p.Source.UnsolvedPredicates(semTable)
}

// CheckValid implements the LogicalOperator interface
func (p *Horizon) CheckValid() error {
	return p.Source.CheckValid()
}

// PushPredicate implements the LogicalOperator interface
func (p *Horizon) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	newSrc, err := p.Source.PushPredicate(expr, semTable)
	if err != nil {
		return nil, err
	}
	p.Source = newSrc

	return p, nil
}

// Compact implements the LogicalOperator interface
func (p *Horizon) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	return p, nil
}
