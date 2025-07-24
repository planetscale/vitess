/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*OrderedAggregate)(nil)

// =============================================================================
// ORDERED AGGREGATE PRIMITIVE
// =============================================================================

// OrderedAggregate is a primitive that performs aggregation on pre-sorted input data.
// It expects the underlying primitive to feed results in an order sorted by the GroupByKeys.
// Rows with duplicate keys are aggregated using the specified Aggregate functions.
//
// This primitive is optimized for cases where the input is already sorted (e.g., from
// a scatter select with ORDER BY), allowing for streaming aggregation without needing
// to buffer all rows in memory.
//
// Key characteristics:
// - Input must be pre-sorted by the GroupByKeys
// - Processes rows in streaming fashion
// - Emits aggregated results as soon as a group boundary is detected
type OrderedAggregate struct {
	Aggregates          []*AggregateParams // Aggregation functions to apply to each group
	GroupByKeys         []*GroupByParams   // Columns that define group boundaries (must be pre-sorted)
	TruncateColumnCount int                // Number of columns to return (0 = no truncation)
	Input               Primitive          // Source primitive providing pre-sorted data
}

// =============================================================================
// GROUP BY CONFIGURATION
// =============================================================================

// GroupByParams specifies a single grouping key column for ordered aggregation.
// It contains information needed to compare values and determine group boundaries.
type GroupByParams struct {
	// Column configuration
	KeyCol          int // Primary column index for grouping
	WeightStringCol int // Weight string column for collation-aware grouping (-1 if not used)

	// Metadata
	Expr         sqlparser.Expr          // Original SQL expression for this grouping key
	FromGroupBy  bool                    // Whether this comes from an explicit GROUP BY clause
	Type         evalengine.Type         // Type information for proper comparison
	CollationEnv *collations.Environment // Collation environment for text comparisons
}

// String returns a human-readable representation of the grouping parameters.
// Used for plan descriptions and debugging.
// Format: column[|weight_column] [COLLATE collation]
func (gbp GroupByParams) String() string {
	var out string

	// Format column specification
	if gbp.WeightStringCol == -1 || gbp.KeyCol == gbp.WeightStringCol {
		// No separate weight string column or same as key column
		out = strconv.Itoa(gbp.KeyCol)
	} else {
		// Include both key column and weight string column
		out = fmt.Sprintf("(%d|%d)", gbp.KeyCol, gbp.WeightStringCol)
	}

	// Add collation information if applicable
	if sqltypes.IsText(gbp.Type.Type()) && gbp.Type.Collation() != collations.Unknown {
		out += " COLLATE " + gbp.CollationEnv.LookupName(gbp.Type.Collation())
	}

	return out
}

// =============================================================================
// PRIMITIVE INTERFACE IMPLEMENTATION
// =============================================================================

// TryExecute implements the Primitive interface for batch execution.
// It executes the ordered aggregation and returns the complete result set.
func (oa *OrderedAggregate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	qr, err := oa.execute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(oa.TruncateColumnCount), nil
}

// =============================================================================
// EXECUTION METHODS
// =============================================================================

// executeGroupBy handles the case where no aggregation functions are specified.
// It performs simple GROUP BY processing, returning the first row from each group.
func (oa *OrderedAggregate) executeGroupBy(result *sqltypes.Result) (*sqltypes.Result, error) {
	// Handle empty result set
	if len(result.Rows) < 1 {
		return result, nil
	}

	// Prepare output result with same fields but reset rows
	out := &sqltypes.Result{
		Fields: result.Fields,
		Rows:   result.Rows[:0],
	}

	var currentKey []sqltypes.Value
	var lastRow sqltypes.Row
	var err error

	// Process each row and detect group boundaries
	for _, row := range result.Rows {
		var nextGroup bool

		currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
		if err != nil {
			return nil, err
		}

		// If we've moved to a new group, emit the representative row from the previous group
		if nextGroup {
			out.Rows = append(out.Rows, lastRow)
		}
		lastRow = row
	}

	// Emit the last group's representative row
	out.Rows = append(out.Rows, lastRow)
	return out, nil
}

// execute performs the main ordered aggregation logic for batch execution.
// It handles both simple GROUP BY (no aggregates) and full aggregation cases.
func (oa *OrderedAggregate) execute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// Execute the input primitive to get the pre-sorted data
	result, err := vcursor.ExecutePrimitive(
		ctx,
		oa.Input,
		bindVars,
		true, // wantFields - we need input field types to correctly calculate output types
	)
	if err != nil {
		return nil, err
	}

	// If no aggregation functions are specified, handle as simple GROUP BY
	if len(oa.Aggregates) == 0 {
		return oa.executeGroupBy(result)
	}

	// Set up aggregation state
	agg, fields, err := newAggregation(result.Fields, oa.Aggregates)
	if err != nil {
		return nil, err
	}

	// Prepare output result
	out := &sqltypes.Result{
		Fields: fields,
		Rows:   make([][]sqltypes.Value, 0, len(result.Rows)),
	}

	// Process rows and perform aggregation
	var currentKey []sqltypes.Value
	for _, row := range result.Rows {
		var nextGroup bool

		// Check if we've moved to a new group
		currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
		if err != nil {
			return nil, err
		}

		// If new group detected, finalize the previous group and reset aggregators
		if nextGroup {
			out.Rows = append(out.Rows, agg.finish())
			agg.reset()
		}

		// Add current row to aggregation state
		if err := agg.add(row); err != nil {
			return nil, err
		}
	}

	// Finalize the last group if we processed any data
	if currentKey != nil {
		out.Rows = append(out.Rows, agg.finish())
	}

	return out, nil
}

// =============================================================================
// STREAMING EXECUTION METHODS
// =============================================================================

// executeStreamGroupBy handles streaming GROUP BY without aggregation functions.
// It emits the first row from each group as it detects group boundaries.
func (oa *OrderedAggregate) executeStreamGroupBy(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	// Use shared utility function to create truncation wrapper
	cb := createTruncationCallback(callback, oa.TruncateColumnCount)

	var fields []*querypb.Field
	var currentKey []sqltypes.Value
	var lastRow sqltypes.Row

	// Visitor function to process each chunk of data from the input
	visitor := func(qr *sqltypes.Result) error {
		var err error

		// Send field definitions on first chunk
		if fields == nil && len(qr.Fields) > 0 {
			fields = qr.Fields
			if err = cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}

		// Process each row in the current chunk
		for _, row := range qr.Rows {
			var nextGroup bool

			// Check for group boundary
			currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
			if err != nil {
				return err
			}

			if nextGroup {
				// New group detected - emit the representative row from the previous group
				if err := cb(&sqltypes.Result{Rows: []sqltypes.Row{lastRow}}); err != nil {
					return err
				}
			}

			lastRow = row
		}
		return nil
	}

	// Execute the input primitive in streaming mode
	// We need field types to correctly calculate output types
	err := vcursor.StreamExecutePrimitive(ctx, oa.Input, bindVars, true, visitor)
	if err != nil {
		return err
	}

	// Emit the last group's representative row
	if lastRow != nil {
		if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{lastRow}}); err != nil {
			return err
		}
	}
	return nil
}

// TryStreamExecute implements the Primitive interface for streaming execution.
// It performs ordered aggregation in streaming mode, emitting results as group boundaries are detected.
func (oa *OrderedAggregate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	// If no aggregation functions, delegate to simple GROUP BY streaming
	if len(oa.Aggregates) == 0 {
		return oa.executeStreamGroupBy(ctx, vcursor, bindVars, callback)
	}

	// Use shared utility function to create truncation wrapper
	cb := createTruncationCallback(callback, oa.TruncateColumnCount)

	var agg aggregationState
	var fields []*querypb.Field
	var currentKey []sqltypes.Value

	// Visitor function to process each chunk of data from the input
	visitor := func(qr *sqltypes.Result) error {
		var err error

		// Initialize aggregation state on first chunk with field definitions
		if agg == nil && len(qr.Fields) != 0 {
			agg, fields, err = newAggregation(qr.Fields, oa.Aggregates)
			if err != nil {
				return err
			}
			// Send field definitions to callback
			if err = cb(&sqltypes.Result{Fields: fields}); err != nil {
				return err
			}
		}

		// Process each row in the current chunk (similar to batch Execute logic)
		for _, row := range qr.Rows {
			var nextGroup bool

			// Check for group boundary
			currentKey, nextGroup, err = oa.nextGroupBy(currentKey, row)
			if err != nil {
				return err
			}

			if nextGroup {
				// New group detected - emit the aggregated result from the previous group
				if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{agg.finish()}}); err != nil {
					return err
				}

				// Reset aggregation state for the new group
				agg.reset()
			}

			// Add current row to aggregation state
			if err := agg.add(row); err != nil {
				return err
			}
		}
		return nil
	}

	// Execute the input primitive in streaming mode
	// We need field types to correctly calculate output types
	err := vcursor.StreamExecutePrimitive(ctx, oa.Input, bindVars, true, visitor)
	if err != nil {
		return err
	}

	// Emit the final aggregated result if we processed any data
	if currentKey != nil {
		if err := cb(&sqltypes.Result{Rows: [][]sqltypes.Value{agg.finish()}}); err != nil {
			return err
		}
	}
	return nil
}

// =============================================================================
// PRIMITIVE METADATA METHODS
// =============================================================================

// GetFields implements the Primitive interface.
// It returns the field definitions that this primitive will output.
func (oa *OrderedAggregate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// Get field definitions from the input primitive
	qr, err := oa.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	// Use shared utility function to transform and truncate fields
	return transformFieldsForAggregation(qr.Fields, oa.Aggregates, oa.TruncateColumnCount)
}

// Inputs returns the input primitives for this aggregation.
// OrderedAggregate has exactly one input primitive.
func (oa *OrderedAggregate) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{oa.Input}, nil
}

// NeedsTransaction implements the Primitive interface.
// It delegates the transaction requirement to its input primitive.
func (oa *OrderedAggregate) NeedsTransaction() bool {
	return oa.Input.NeedsTransaction()
}

// =============================================================================
// GROUP BOUNDARY DETECTION
// =============================================================================

// nextGroupBy determines whether the next row belongs to a new group by comparing
// the grouping key values between the current group and the next row.
//
// Returns:
// - nextKey: The grouping key values from the next row (becomes the new current key)
// - nextGroup: true if the next row starts a new group, false if it belongs to the current group
// - err: any error encountered during comparison
func (oa *OrderedAggregate) nextGroupBy(currentKey, nextRow []sqltypes.Value) (nextKey []sqltypes.Value, nextGroup bool, err error) {
	// First row always starts the first group
	if currentKey == nil {
		return nextRow, false, nil
	}

	// Compare each grouping key column to detect group boundaries
	for _, gb := range oa.GroupByKeys {
		v1 := currentKey[gb.KeyCol]
		v2 := nextRow[gb.KeyCol]

		// Quick comparison using TinyWeightCmp (faster for common cases)
		if v1.TinyWeightCmp(v2) != 0 {
			return nextRow, true, nil
		}

		// Full comparison for potentially equal values
		cmp, err := evalengine.NullsafeCompare(v1, v2, gb.CollationEnv, gb.Type.Collation(), gb.Type.Values())
		if err != nil {
			// Handle collation errors by falling back to weight string column
			_, isCollationErr := err.(evalengine.UnsupportedCollationError)
			if !isCollationErr || gb.WeightStringCol == -1 {
				return nil, false, err
			}

			// Fall back to weight string comparison
			gb.KeyCol = gb.WeightStringCol
			cmp, err = evalengine.NullsafeCompare(currentKey[gb.WeightStringCol], nextRow[gb.WeightStringCol], gb.CollationEnv, gb.Type.Collation(), gb.Type.Values())
			if err != nil {
				return nil, false, err
			}
		}

		// If any grouping key differs, we have a new group
		if cmp != 0 {
			return nextRow, true, nil
		}
	}

	// All grouping keys match - same group
	return currentKey, false, nil
}

// =============================================================================
// UTILITY FUNCTIONS AND DESCRIPTION
// =============================================================================

// aggregateParamsToString converts AggregateParams to string for display purposes
func aggregateParamsToString(in any) string {
	return in.(*AggregateParams).String()
}

// groupByParamsToString converts GroupByParams to string for display purposes
func groupByParamsToString(i any) string {
	return i.(*GroupByParams).String()
}

// description implements the Primitive interface.
// It returns a structured description of this primitive for explain plans and debugging.
func (oa *OrderedAggregate) description() PrimitiveDescription {
	// Format aggregation and grouping parameters for display
	aggregates := GenericJoin(oa.Aggregates, aggregateParamsToString)
	groupBy := GenericJoin(oa.GroupByKeys, groupByParamsToString)

	// Build the description metadata
	other := map[string]any{
		"Aggregates": aggregates,
		"GroupBy":    groupBy,
	}

	// Include column truncation information if applicable
	if oa.TruncateColumnCount > 0 {
		other["ResultColumns"] = oa.TruncateColumnCount
	}

	return PrimitiveDescription{
		OperatorType: "Aggregate",
		Variant:      "Ordered",
		Other:        other,
	}
}
