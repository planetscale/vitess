/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// =============================================================================
// AGGREGATE PARAMETERS AND CONFIGURATION
// =============================================================================

// AggregateParams specifies the parameters for each aggregation function.
// It contains the operation code, input column information, and metadata
// needed to perform the aggregation correctly.
type AggregateParams struct {
	// Core aggregation configuration
	Opcode opcode.AggregateOpcode // The aggregation operation to perform
	Col    int                    // Input column number for the aggregation

	// Distinct operation configuration
	// These fields are used only for DISTINCT aggregations (COUNT DISTINCT, SUM DISTINCT, etc.)
	KeyCol int             // Column to use for distinct comparison
	WCol   int             // Weight string column for collation-aware distinct operations
	Type   evalengine.Type // Type information for proper comparison

	// Output and display configuration
	Alias    string                 // Output column alias
	Func     sqlparser.AggrFunc     // Original SQL function for metadata
	Original *sqlparser.AliasedExpr // Original SQL expression

	// Original operation tracking
	// This tracks the original function from the SQL query, which may differ
	// from the engine-level operation (e.g., AVG becomes SUM+COUNT internally)
	OrigOpcode opcode.AggregateOpcode

	// Collation support
	CollationEnv *collations.Environment // Environment for collation-aware operations
}

// NewAggregateParam creates a new AggregateParams with the specified configuration.
// For operations that need comparable values (like DISTINCT), it automatically
// sets the KeyCol to the same column as the input column.
func NewAggregateParam(opcode opcode.AggregateOpcode, col int, alias string, collationEnv *collations.Environment) *AggregateParams {
	out := &AggregateParams{
		Opcode:       opcode,
		Col:          col,
		Alias:        alias,
		WCol:         -1, // -1 indicates no weight string column assigned
		CollationEnv: collationEnv,
	}

	// For operations that need comparable values (DISTINCT operations),
	// set the key column to the same as the input column by default
	if opcode.NeedsComparableValues() {
		out.KeyCol = col
	}

	return out
}

// WAssigned returns true if a weight string column has been assigned
// for collation-aware distinct operations.
func (ap *AggregateParams) WAssigned() bool {
	return ap.WCol >= 0
}

// String returns a human-readable representation of the aggregate parameters.
// Format: OPERATION[_ORIGINAL_OP](column[|weight_col]) [COLLATE collation] [AS alias]
func (ap *AggregateParams) String() string {
	// Build column specification
	keyCol := strconv.Itoa(ap.Col)
	if ap.WAssigned() {
		// Include weight string column for collation-aware operations
		keyCol = fmt.Sprintf("%s|%d", keyCol, ap.WCol)
	}

	// Add collation information if applicable
	if sqltypes.IsText(ap.Type.Type()) && ap.CollationEnv.IsSupported(ap.Type.Collation()) {
		keyCol += " COLLATE " + ap.CollationEnv.LookupName(ap.Type.Collation())
	}

	// Include original operation if different from current operation
	dispOrigOp := ""
	if ap.OrigOpcode != opcode.AggregateUnassigned && ap.OrigOpcode != ap.Opcode {
		dispOrigOp = "_" + ap.OrigOpcode.String()
	}

	// Format final string with optional alias
	if ap.Alias != "" {
		return fmt.Sprintf("%s%s(%s) AS %s", ap.Opcode.String(), dispOrigOp, keyCol, ap.Alias)
	}
	return fmt.Sprintf("%s%s(%s)", ap.Opcode.String(), dispOrigOp, keyCol)
}

// typ returns the SQL type that this aggregation will produce given the input type.
// If an original operation is specified, it uses that for type determination,
// otherwise it uses the current operation code.
func (ap *AggregateParams) typ(inputType querypb.Type) querypb.Type {
	if ap.OrigOpcode != opcode.AggregateUnassigned {
		return ap.OrigOpcode.SQLType(inputType)
	}
	return ap.Opcode.SQLType(inputType)
}

// =============================================================================
// AGGREGATOR INTERFACE AND IMPLEMENTATIONS
// =============================================================================

// aggregator defines the interface for all aggregation operations.
// Each aggregator maintains its own state and can process rows incrementally.
type aggregator interface {
	// add processes a single row and updates the aggregation state
	add(row []sqltypes.Value) error
	// finish returns the final aggregated value
	finish() sqltypes.Value
	// reset clears the aggregation state for reuse
	reset()
}

// aggregatorDistinct handles the distinct logic for COUNT DISTINCT and SUM DISTINCT operations.
// It tracks the last seen value and determines if a new value should be included in the aggregation.
type aggregatorDistinct struct {
	column       int                       // Column index to check for distinct values (-1 if disabled)
	last         sqltypes.Value            // Last processed value for comparison
	coll         collations.ID             // Collation ID for text comparison
	collationEnv *collations.Environment   // Collation environment for comparisons
	values       *evalengine.EnumSetValues // Enum/Set values for proper comparison
}

// shouldReturn determines if the current row should be skipped because its value
// is the same as the previously processed value (for DISTINCT operations).
// Returns true if the row should be skipped, false if it should be processed.
func (a *aggregatorDistinct) shouldReturn(row []sqltypes.Value) (bool, error) {
	// If no distinct column is configured, never skip rows
	if a.column < 0 {
		return false, nil
	}

	last := a.last
	next := row[a.column]

	// If we have a previous value, compare it with the current value
	if !last.IsNull() {
		// First do a quick comparison using TinyWeightCmp
		if last.TinyWeightCmp(next) == 0 {
			// If they might be equal, do a full comparison
			cmp, err := evalengine.NullsafeCompare(last, next, a.collationEnv, a.coll, a.values)
			if err != nil {
				return true, err
			}
			// If values are equal, skip this row
			if cmp == 0 {
				return true, nil
			}
		}
	}

	// Update the last seen value and process this row
	a.last = next
	return false, nil
}

// reset clears the distinct tracking state
func (a *aggregatorDistinct) reset() {
	a.last = sqltypes.NULL
}

// =============================================================================
// COUNT AGGREGATORS
// =============================================================================

// aggregatorCount implements COUNT(column) and COUNT(DISTINCT column) operations.
// It counts non-NULL values, optionally filtering for distinct values only.
type aggregatorCount struct {
	from     int                // Source column index
	n        int64              // Current count
	distinct aggregatorDistinct // Distinct value tracker (for COUNT DISTINCT)
}

// add processes a row for COUNT aggregation, skipping NULL values and duplicates (if DISTINCT)
func (a *aggregatorCount) add(row []sqltypes.Value) error {
	// COUNT ignores NULL values
	if row[a.from].IsNull() {
		return nil
	}

	// For COUNT DISTINCT, skip duplicate values
	if ret, err := a.distinct.shouldReturn(row); ret {
		return err
	}

	// Increment the count
	a.n++
	return nil
}

// finish returns the final count as an INT64 value
func (a *aggregatorCount) finish() sqltypes.Value {
	return sqltypes.NewInt64(a.n)
}

// reset clears the count and distinct tracking state
func (a *aggregatorCount) reset() {
	a.n = 0
	a.distinct.reset()
}

// aggregatorCountStar implements COUNT(*) operations.
// Unlike COUNT(column), it counts all rows including those with NULL values.
type aggregatorCountStar struct {
	n int64 // Current count of all rows
}

// add increments the count for every row (COUNT(*) counts all rows)
func (a *aggregatorCountStar) add(_ []sqltypes.Value) error {
	a.n++
	return nil
}

// finish returns the final count as an INT64 value
func (a *aggregatorCountStar) finish() sqltypes.Value {
	return sqltypes.NewInt64(a.n)
}

// reset clears the count
func (a *aggregatorCountStar) reset() {
	a.n = 0
}

// =============================================================================
// MIN/MAX AGGREGATORS
// =============================================================================

// aggregatorMinMax provides the common functionality for MIN and MAX operations.
// It uses the evalengine.MinMax for proper type-aware comparisons.
type aggregatorMinMax struct {
	from   int               // Source column index
	minmax evalengine.MinMax // Engine for min/max calculations
}

// aggregatorMin implements MIN(column) operations.
// It finds the minimum value among all non-NULL values in the specified column.
type aggregatorMin struct {
	aggregatorMinMax
}

// add processes a row for MIN aggregation
func (a *aggregatorMin) add(row []sqltypes.Value) (err error) {
	return a.minmax.Min(row[a.from])
}

// aggregatorMax implements MAX(column) operations.
// It finds the maximum value among all non-NULL values in the specified column.
type aggregatorMax struct {
	aggregatorMinMax
}

// add processes a row for MAX aggregation
func (a *aggregatorMax) add(row []sqltypes.Value) (err error) {
	return a.minmax.Max(row[a.from])
}

// finish returns the final min/max value
func (a *aggregatorMinMax) finish() sqltypes.Value {
	return a.minmax.Result()
}

// reset clears the min/max tracking state
func (a *aggregatorMinMax) reset() {
	a.minmax.Reset()
}

// =============================================================================
// SUM AGGREGATOR
// =============================================================================

// aggregatorSum implements SUM(column) and SUM(DISTINCT column) operations.
// It calculates the sum of all non-NULL numeric values, optionally filtering for distinct values.
type aggregatorSum struct {
	from     int                // Source column index
	sum      evalengine.Sum     // Engine for sum calculations
	distinct aggregatorDistinct // Distinct value tracker (for SUM DISTINCT)
}

// add processes a row for SUM aggregation, skipping NULL values and duplicates (if DISTINCT)
func (a *aggregatorSum) add(row []sqltypes.Value) error {
	// SUM ignores NULL values
	if row[a.from].IsNull() {
		return nil
	}

	// For SUM DISTINCT, skip duplicate values
	if ret, err := a.distinct.shouldReturn(row); ret {
		return err
	}

	// Add the value to the running sum
	return a.sum.Add(row[a.from])
}

// finish returns the final sum value
func (a *aggregatorSum) finish() sqltypes.Value {
	return a.sum.Result()
}

// reset clears the sum and distinct tracking state
func (a *aggregatorSum) reset() {
	a.sum.Reset()
	a.distinct.reset()
}

// =============================================================================
// SCALAR AND OTHER AGGREGATORS
// =============================================================================

// aggregatorScalar implements ANY_VALUE(column) operations and serves as a pass-through
// for non-aggregated columns in GROUP BY queries. It returns the first non-NULL value encountered.
type aggregatorScalar struct {
	from    int            // Source column index
	current sqltypes.Value // The first value encountered
	init    bool           // Whether we've seen our first value
}

// add captures the first value from the specified column
func (a *aggregatorScalar) add(row []sqltypes.Value) error {
	if !a.init {
		a.current = row[a.from]
		a.init = true
	}
	return nil
}

// finish returns the captured value
func (a *aggregatorScalar) finish() sqltypes.Value {
	return a.current
}

// reset clears the captured value
func (a *aggregatorScalar) reset() {
	a.current = sqltypes.NULL
	a.init = false
}

// aggregatorGroupConcat implements GROUP_CONCAT operations.
// It concatenates all non-NULL string values from a column with a specified separator.
type aggregatorGroupConcat struct {
	from      int           // Source column index
	type_     sqltypes.Type // Output type for the concatenated result
	separator []byte        // Separator to use between values

	concat []byte // Accumulated concatenated result
	n      int    // Number of values processed
}

// add processes a row for GROUP_CONCAT, concatenating non-NULL values with separators
func (a *aggregatorGroupConcat) add(row []sqltypes.Value) error {
	// GROUP_CONCAT ignores NULL values
	if row[a.from].IsNull() {
		return nil
	}

	// Add separator before the value (except for the first value)
	if a.n > 0 {
		a.concat = append(a.concat, a.separator...)
	}

	// Append the raw bytes of the current value
	a.concat = append(a.concat, row[a.from].Raw()...)
	a.n++
	return nil
}

// finish returns the concatenated result, or NULL if no values were processed
func (a *aggregatorGroupConcat) finish() sqltypes.Value {
	if a.n == 0 {
		return sqltypes.NULL
	}
	return sqltypes.MakeTrusted(a.type_, a.concat)
}

// reset clears the concatenation state
func (a *aggregatorGroupConcat) reset() {
	a.n = 0
	// Cannot reuse this byte slice as it's returned as MakeTrusted
	a.concat = nil
}

// aggregatorGtid implements GTID aggregation for replication operations.
// It collects GTID information from multiple shards into a single VGtid structure.
type aggregatorGtid struct {
	from   int                       // Source column index (GTID column)
	shards []*binlogdatapb.ShardGtid // Collected shard GTID information
}

// add processes a row for GTID aggregation, collecting keyspace, shard, and GTID information
// Expects the GTID column at 'from', keyspace at 'from-1', and shard at 'from+1'
func (a *aggregatorGtid) add(row []sqltypes.Value) error {
	a.shards = append(a.shards, &binlogdatapb.ShardGtid{
		Keyspace: row[a.from-1].ToString(), // Keyspace from previous column
		Shard:    row[a.from+1].ToString(), // Shard from next column
		Gtid:     row[a.from].ToString(),   // GTID from current column
	})
	return nil
}

// finish returns the aggregated GTID as a VGtid string representation
func (a *aggregatorGtid) finish() sqltypes.Value {
	gtid := binlogdatapb.VGtid{ShardGtids: a.shards}
	return sqltypes.NewVarChar(gtid.String())
}

// reset clears the collected shard GTID information
func (a *aggregatorGtid) reset() {
	// Safe to reuse because only the serialized form is returned
	a.shards = a.shards[:0]
}

// =============================================================================
// AGGREGATION STATE MANAGEMENT
// =============================================================================

// aggregationState represents the complete state of all aggregations for a single group.
// It contains one aggregator per output column.
type aggregationState []aggregator

// add processes a single row through all aggregators in this state
func (a aggregationState) add(row []sqltypes.Value) error {
	for _, st := range a {
		if err := st.add(row); err != nil {
			return err
		}
	}
	return nil
}

// finish collects the final results from all aggregators to form the output row
func (a aggregationState) finish() (row []sqltypes.Value) {
	row = make([]sqltypes.Value, 0, len(a))
	for _, st := range a {
		row = append(row, st.finish())
	}
	return
}

// reset clears all aggregation state for reuse
func (a aggregationState) reset() {
	for _, st := range a {
		st.reset()
	}
}

// =============================================================================
// SHARED UTILITY FUNCTIONS
// =============================================================================

// createTruncationCallback creates a wrapper function that applies column truncation
// before calling the user-provided callback. This is used by both OrderedAggregate
// and ScalarAggregate for consistent truncation behavior.
func createTruncationCallback(callback func(*sqltypes.Result) error, truncateCount int) func(*sqltypes.Result) error {
	return func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(truncateCount))
	}
}

// transformFieldsForAggregation is a helper function that transforms input field definitions
// based on aggregation parameters. It's used by both OrderedAggregate and ScalarAggregate
// in their GetFields implementations.
func transformFieldsForAggregation(inputFields []*querypb.Field, aggregates []*AggregateParams, truncateCount int) (*sqltypes.Result, error) {
	// Transform field definitions based on aggregation parameters
	_, fields, err := newAggregation(inputFields, aggregates)
	if err != nil {
		return nil, err
	}

	// Return the transformed fields with truncation applied
	qr := &sqltypes.Result{Fields: fields}
	return qr.Truncate(truncateCount), nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// isComparable determines if a SQL type can be used in comparison operations (MIN/MAX).
// This is used to validate that MIN/MAX operations are only applied to comparable types.
func isComparable(typ sqltypes.Type) bool {
	// NULL, numeric, and binary types are always comparable
	if typ == sqltypes.Null || sqltypes.IsNumber(typ) || sqltypes.IsBinary(typ) {
		return true
	}

	// Specific temporal and other comparable types
	switch typ {
	case sqltypes.Timestamp,
		sqltypes.Date,
		sqltypes.Time,
		sqltypes.Datetime,
		sqltypes.Enum,
		sqltypes.Set,
		sqltypes.TypeJSON,
		sqltypes.Bit,
		sqltypes.Vector:
		return true
	}

	return false
}

// =============================================================================
// AGGREGATION FACTORY
// =============================================================================

// newAggregation creates a new aggregation state from field definitions and aggregation parameters.
// It returns the aggregation state, updated field definitions, and any errors encountered.
//
// This function performs the following steps:
// 1. Clones the input field definitions to avoid modifying the originals
// 2. Creates appropriate aggregator instances based on the operation codes
// 3. Validates that the operations are supported for the given data types
// 4. Updates field types and names based on the aggregation results
// 5. Fills in scalar aggregators for non-aggregated columns
func newAggregation(fields []*querypb.Field, aggregates []*AggregateParams) (aggregationState, []*querypb.Field, error) {
	// Clone field definitions to avoid modifying the originals
	fields = slice.Map(fields, func(from *querypb.Field) *querypb.Field { return from.CloneVT() })

	// Initialize aggregator array with one slot per output column
	agstate := make([]aggregator, len(fields))

	// Process each aggregation parameter to create appropriate aggregators
	for _, aggr := range aggregates {
		sourceType := fields[aggr.Col].Type
		targetType := aggr.typ(sourceType)

		var ag aggregator
		var distinct = -1

		// Configure distinct column for operations that support it
		if aggr.Opcode.IsDistinct() {
			distinct = aggr.KeyCol
			// For non-comparable types, use the weight string column if available
			if aggr.WAssigned() && !isComparable(sourceType) {
				distinct = aggr.WCol
			}
		}

		// Validate MIN/MAX operations on comparable types
		if aggr.Opcode == opcode.AggregateMin || aggr.Opcode == opcode.AggregateMax {
			if aggr.WAssigned() && !isComparable(sourceType) {
				return nil, nil, vterrors.VT12001("min/max on types that are not comparable is not supported")
			}
		}

		// Create the appropriate aggregator based on the operation code
		switch aggr.Opcode {
		case opcode.AggregateCountStar:
			// COUNT(*) - counts all rows including those with NULL values
			ag = &aggregatorCountStar{}

		case opcode.AggregateCount, opcode.AggregateCountDistinct:
			// COUNT(col) and COUNT(DISTINCT col) - counts non-NULL values
			ag = &aggregatorCount{
				from: aggr.Col,
				distinct: aggregatorDistinct{
					column:       distinct,
					coll:         aggr.Type.Collation(),
					collationEnv: aggr.CollationEnv,
					values:       aggr.Type.Values(),
				},
			}

		case opcode.AggregateSum, opcode.AggregateSumDistinct:
			// SUM(col) and SUM(DISTINCT col) - sums numeric values
			var sum evalengine.Sum
			// Special handling for summing count results (for distributed aggregation)
			switch aggr.OrigOpcode {
			case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct:
				sum = evalengine.NewSumOfCounts()
			default:
				sum = evalengine.NewAggregationSum(sourceType)
			}

			ag = &aggregatorSum{
				from: aggr.Col,
				sum:  sum,
				distinct: aggregatorDistinct{
					column:       distinct,
					coll:         aggr.Type.Collation(),
					collationEnv: aggr.CollationEnv,
					values:       aggr.Type.Values(),
				},
			}

		case opcode.AggregateMin:
			// MIN(col) - finds the minimum value
			ag = &aggregatorMin{
				aggregatorMinMax{
					from:   aggr.Col,
					minmax: evalengine.NewAggregationMinMax(sourceType, aggr.CollationEnv, aggr.Type.Collation(), aggr.Type.Values()),
				},
			}

		case opcode.AggregateMax:
			// MAX(col) - finds the maximum value
			ag = &aggregatorMax{
				aggregatorMinMax{
					from:   aggr.Col,
					minmax: evalengine.NewAggregationMinMax(sourceType, aggr.CollationEnv, aggr.Type.Collation(), aggr.Type.Values()),
				},
			}

		case opcode.AggregateGtid:
			// GTID aggregation for replication
			ag = &aggregatorGtid{from: aggr.Col}

		case opcode.AggregateAnyValue:
			// ANY_VALUE(col) - returns any value from the group
			ag = &aggregatorScalar{from: aggr.Col}

		case opcode.AggregateGroupConcat:
			// GROUP_CONCAT - concatenates values with a separator
			gcFunc := aggr.Func.(*sqlparser.GroupConcatExpr)
			separator := []byte(gcFunc.Separator)
			ag = &aggregatorGroupConcat{
				from:      aggr.Col,
				type_:     targetType,
				separator: separator,
			}

		default:
			panic("BUG: unexpected Aggregation opcode")
		}

		// Install the aggregator and update field metadata
		agstate[aggr.Col] = ag
		fields[aggr.Col].Type = targetType
		if aggr.Alias != "" {
			fields[aggr.Col].Name = aggr.Alias
		}
	}

	// Fill in scalar aggregators for non-aggregated columns
	// These pass through the first value encountered
	for i, a := range agstate {
		if a == nil {
			agstate[i] = &aggregatorScalar{from: i}
		}
	}

	return agstate, fields, nil
}
