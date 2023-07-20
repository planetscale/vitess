package engine

import (
	"context"
	"encoding/json"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtboostpb "vitess.io/vitess/go/vt/proto/vtboost"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type BoostCompare struct {
	Boost      Primitive
	Gen4       Primitive
	HasOrderBy bool

	failureMode vtboostpb.Science_FailureMode
}

var _ Primitive = (*BoostCompare)(nil)

func (b *BoostCompare) RouteType() string {
	return "BoostCompare"
}

func (b *BoostCompare) GetKeyspaceName() string {
	return b.Boost.GetKeyspaceName()
}

func (b *BoostCompare) GetTableName() string {
	return b.Boost.GetTableName()
}

func (b *BoostCompare) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return b.Boost.GetFields(ctx, vcursor, bindVars)
}

func (b *BoostCompare) NeedsTransaction() bool {
	return false
}

func (b *BoostCompare) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	boostResult, boostErr := b.Boost.TryExecute(ctx, vcursor, bindVars, wantfields)

	switch b.failureMode {
	case vtboostpb.Science_LOG:
		go func() {
			// we can ignore the error and the results, the method will take care of logging the mismatch
			_, _ = b.executeAndCompareWithGen4(context.Background(), vcursor, bindVars, wantfields, boostErr, boostResult)
		}()
	case vtboostpb.Science_ERROR:
		return b.executeAndCompareWithGen4(ctx, vcursor, bindVars, wantfields, boostErr, boostResult)
	}
	if boostErr != nil {
		return nil, boostErr
	}
	return boostResult, nil
}

func (b *BoostCompare) executeAndCompareWithGen4(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, boostErr error, boostResult *sqltypes.Result) (*sqltypes.Result, error) {
	gen4Result, gen4Err := b.Gen4.TryExecute(ctx, vcursor, bindVars, wantfields)

	if err := CompareErrors(gen4Err, boostErr, "Gen4", "Boost"); err != nil {
		log.Error(err.Error())
		if b.failureMode == vtboostpb.Science_ERROR {
			// we want to return the error here
			// the error from CompareErrors is more explicit and contains a diff
			return nil, err
		}
		// we don't want to error out because of the diff, so we simply return the boost error
		if boostErr != nil {
			return nil, boostErr
		}
		// Gen4 failed and Boost did not, no need to compare the results, we can return
		return boostResult, nil
	}

	if err := b.compareResults(boostResult, gen4Result); err != nil {
		printMismatch(gen4Result, boostResult, b.Gen4, b.Boost, "Gen4", "Boost")
		if b.failureMode == vtboostpb.Science_ERROR {
			return nil, err
		}
	}
	return boostResult, nil
}

func (b *BoostCompare) TryStreamExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
) error {
	var mu sync.Mutex
	var boostErr error
	boostResult := &sqltypes.Result{}

	boostErr = b.Boost.TryStreamExecute(ctx, vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		boostResult.AppendResult(result)
		return nil
	})

	switch b.failureMode {
	case vtboostpb.Science_LOG:
		go func() {
			_, _ = b.executeAndCompareStreamWithGen4(context.Background(), vcursor, bindVars, wantfields, boostResult, boostErr)
		}()
	case vtboostpb.Science_ERROR:
		result, err := b.executeAndCompareStreamWithGen4(ctx, vcursor, bindVars, wantfields, boostResult, boostErr)
		if err != nil {
			return err
		}
		return callback(result)
	}
	if boostErr != nil {
		return boostErr
	}
	return callback(boostResult)
}

func (b *BoostCompare) executeAndCompareStreamWithGen4(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, boostResult *sqltypes.Result, boostErr error) (*sqltypes.Result, error) {
	var mu sync.Mutex
	var gen4Err error
	gen4Result := &sqltypes.Result{}

	gen4Err = b.Gen4.TryStreamExecute(ctx, vcursor, bindVars, wantfields, func(r *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		gen4Result.AppendResult(r)
		return nil
	})

	if err := CompareErrors(gen4Err, boostErr, "Gen4", "Boost"); err != nil {
		log.Errorf(err.Error())
		if b.failureMode == vtboostpb.Science_ERROR {
			// we want to return the error here
			// the error from CompareErrors is more explicit and contains a diff
			return nil, err
		}
		// we don't want to error out because of the diff, so we simply return the boost error
		if boostErr != nil {
			return nil, boostErr
		}
		// Gen4 failed and Boost did not, no need to compare the results, we can return
		return boostResult, nil
	}

	if err := b.compareResults(boostResult, gen4Result); err != nil {
		printMismatch(gen4Result, boostResult, b.Gen4, b.Boost, "Gen4", "Boost")
		if b.failureMode == vtboostpb.Science_ERROR {
			return nil, err
		}
	}
	return boostResult, nil
}

func (b *BoostCompare) Inputs() []Primitive {
	return []Primitive{b.Gen4, b.Boost}
}

func (b *BoostCompare) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "BoostCompare"}
}

func (b *BoostCompare) compareResults(result *sqltypes.Result, gen4Res *sqltypes.Result) error {
	var match bool
	boost := []sqltypes.Result{*result}
	gen4 := []sqltypes.Result{*gen4Res}
	if b.HasOrderBy {
		match = sqltypes.ResultsEqual(boost, gen4)
	} else {
		match = sqltypes.ResultsEqualUnordered(boost, gen4)
	}
	if !match {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match, see VTGate's logs for more information")
	}
	return nil
}

func printMismatch(leftResult, rightResult *sqltypes.Result, leftPrimitive, rightPrimitive Primitive, leftName, rightName string) {
	log.Errorf("Results of %s and %s are not equal. Displaying diff.", rightName, leftName)

	// get right plan and print it
	rightplan := &Plan{
		Instructions: rightPrimitive,
	}
	rightJSON, _ := json.MarshalIndent(rightplan, "", "  ")
	log.Errorf("%s's plan:\n%s", rightName, string(rightJSON))

	// get left's plan and print it
	leftplan := &Plan{
		Instructions: leftPrimitive,
	}
	leftJSON, _ := json.MarshalIndent(leftplan, "", "  ")
	log.Errorf("%s's plan:\n%s", leftName, string(leftJSON))

	log.Errorf("%s's results:\n", rightName)
	log.Errorf("\t[rows affected: %d]\n", rightResult.RowsAffected)
	for _, row := range rightResult.Rows {
		log.Errorf("\t%s", row)
	}
	log.Errorf("%s's results:\n", leftName)
	log.Errorf("\t[rows affected: %d]\n", leftResult.RowsAffected)
	for _, row := range leftResult.Rows {
		log.Errorf("\t%s", row)
	}
	log.Error("End of diff.")
}

// CompareErrors compares the two errors, and if they don't match, produces an error
func CompareErrors(leftErr, rightErr error, leftName, rightName string) error {
	if leftErr != nil && rightErr != nil {
		if leftErr.Error() == rightErr.Error() {
			return rightErr
		}
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s and %s failed with different errors: %s: [%s], %s: [%s]", leftName, rightName, leftErr.Error(), rightErr.Error(), leftName, rightName)
	}
	if leftErr == nil && rightErr != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s failed while %s did not: %s", rightName, rightErr.Error(), leftName)
	}
	if leftErr != nil && rightErr == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s failed while %s did not: %s", leftName, leftErr.Error(), rightName)
	}
	return nil
}
