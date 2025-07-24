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

package engine

import (
	"context"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*ScalarAggregate)(nil)

// =============================================================================
// SCALAR AGGREGATE PRIMITIVE
// =============================================================================

// ScalarAggregate is a primitive that performs aggregation without grouping keys.
// It applies aggregation functions to the entire result set, producing a single
// output row with aggregated values.
//
// This primitive handles queries like:
// - SELECT COUNT(*) FROM table
// - SELECT SUM(price), AVG(quantity) FROM orders
// - SELECT MAX(created_at) FROM users
//
// Key characteristics:
// - No grouping keys - operates on the entire result set
// - Always produces exactly one output row (even for empty input)
// - Supports both batch and streaming execution modes
// - Thread-safe for streaming execution with proper synchronization
type ScalarAggregate struct {
	// Aggregation configuration
	Aggregates []*AggregateParams // Aggregation functions to apply to the entire result set

	// Output configuration
	TruncateColumnCount int // Number of columns to return (0 = no truncation)

	// Input source
	Input Primitive // Source primitive providing data for aggregation
}

// =============================================================================
// PRIMITIVE INTERFACE IMPLEMENTATION
// =============================================================================

// GetFields implements the Primitive interface.
// It returns the field definitions that this primitive will output after aggregation.
func (sa *ScalarAggregate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// Get field definitions from the input primitive
	qr, err := sa.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	// Use shared utility function to transform and truncate fields
	return transformFieldsForAggregation(qr.Fields, sa.Aggregates, sa.TruncateColumnCount)
}

// NeedsTransaction implements the Primitive interface.
// It delegates the transaction requirement to its input primitive.
func (sa *ScalarAggregate) NeedsTransaction() bool {
	return sa.Input.NeedsTransaction()
}

// =============================================================================
// EXECUTION METHODS
// =============================================================================

// TryExecute implements the Primitive interface for batch execution.
// It executes the scalar aggregation and returns a single row with aggregated results.
func (sa *ScalarAggregate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// Execute the input primitive to get all data
	result, err := vcursor.ExecutePrimitive(ctx, sa.Input, bindVars, true)
	if err != nil {
		return nil, err
	}

	// Set up aggregation state
	agg, fields, err := newAggregation(result.Fields, sa.Aggregates)
	if err != nil {
		return nil, err
	}

	// Process all rows through the aggregators
	for _, row := range result.Rows {
		if err := agg.add(row); err != nil {
			return nil, err
		}
	}

	// Build the final result with a single aggregated row
	out := &sqltypes.Result{
		Fields: fields,
		Rows:   [][]sqltypes.Value{agg.finish()},
	}
	return out.Truncate(sa.TruncateColumnCount), nil
}

// TryStreamExecute implements the Primitive interface for streaming execution.
// It processes data incrementally and emits a single aggregated result at the end.
//
// Note: This method uses synchronization (mutex) because the underlying primitive
// may call the visitor function concurrently, and we need to ensure thread-safe
// access to the shared aggregation state.
func (sa *ScalarAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// Use shared utility function to create truncation wrapper
	cb := createTruncationCallback(callback, sa.TruncateColumnCount)

	// Synchronization for thread-safe aggregation
	var mu sync.Mutex
	var agg aggregationState
	var fields []*querypb.Field
	fieldsSent := !wantfields

	// Stream execute the input primitive with a visitor function
	err := vcursor.StreamExecutePrimitive(ctx, sa.Input, bindVars, true, func(result *sqltypes.Result) error {
		// Synchronize access to shared variables since the underlying primitive
		// may call this function concurrently from multiple goroutines
		mu.Lock()
		defer mu.Unlock()

		// Initialize aggregation state on first chunk with field definitions
		if agg == nil && len(result.Fields) != 0 {
			var err error
			agg, fields, err = newAggregation(result.Fields, sa.Aggregates)
			if err != nil {
				return err
			}
		}

		// Send field definitions if requested and not yet sent
		if !fieldsSent {
			if err := cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
			fieldsSent = true
		}

		// Process all rows in the current chunk
		for _, row := range result.Rows {
			if err := agg.add(row); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Emit the final aggregated result as a single row
	return cb(&sqltypes.Result{Rows: [][]sqltypes.Value{agg.finish()}})
}

// =============================================================================
// PRIMITIVE METADATA METHODS
// =============================================================================

// Inputs returns the input primitives for this aggregation.
// ScalarAggregate has exactly one input primitive.
func (sa *ScalarAggregate) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{sa.Input}, nil
}

// description implements the Primitive interface.
// It returns a structured description of this primitive for explain plans and debugging.
func (sa *ScalarAggregate) description() PrimitiveDescription {
	// Format aggregation parameters for display
	aggregates := GenericJoin(sa.Aggregates, aggregateParamsToString)

	// Build the description metadata
	other := map[string]any{
		"Aggregates": aggregates,
	}

	// Include column truncation information if applicable
	if sa.TruncateColumnCount > 0 {
		other["ResultColumns"] = sa.TruncateColumnCount
	}

	return PrimitiveDescription{
		OperatorType: "Aggregate",
		Variant:      "Scalar",
		Other:        other,
	}
}
