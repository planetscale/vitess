package engine

import (
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Send)(nil)

// Send is an operator to send query to the specific keyspace, tabletType and destination
type Send struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	// NoAutoCommit specifies if we need to check autocommit behaviour
	NoAutoCommit bool

	noInputs
}

// MarshalJSON serializes the Send into a JSON representation.
// It's used for testing and diagnostics.
func (s *Send) MarshalJSON() ([]byte, error) {
	marshalSend := struct {
		Opcode            string
		Keyspace          *vindexes.Keyspace
		TargetDestination key.Destination
		Query             string
		NoAutoCommit      bool
	}{
		Opcode:            "Send",
		Keyspace:          s.Keyspace,
		TargetDestination: s.TargetDestination,
		NoAutoCommit:      s.NoAutoCommit,
		Query:             s.Query,
	}

	return json.Marshal(marshalSend)
}

// RouteType implements Primitive interface
func (s *Send) RouteType() string {
	if s.NoAutoCommit {
		return "SendNoAutoCommit"
	}

	return "Send"
}

// GetKeyspaceName implements Primitive interface
func (s *Send) GetKeyspaceName() string {
	return s.Keyspace.Name
}

// GetTableName implements Primitive interface
func (s *Send) GetTableName() string {
	return ""
}

// Execute implements Primitive interface
func (s *Send) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, _ bool) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(s.Keyspace.Name, nil, []key.Destination{s.TargetDestination})
	if err != nil {
		return nil, vterrors.Wrap(err, "sendExecute")
	}

	if !s.Keyspace.Sharded && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           s.Query,
			BindVariables: bindVars,
		}
	}

	canAutocommit := false
	if !s.NoAutoCommit {
		canAutocommit = len(rss) == 1 && vcursor.AutocommitApproval()
	}

	rollbackOnError := !s.NoAutoCommit // for non-dml queries, there's no need to do a rollback
	result, errs := vcursor.ExecuteMultiShard(rss, queries, rollbackOnError, canAutocommit)
	err = vterrors.Aggregate(errs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// StreamExecute implements Primitive interface
func (s *Send) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable") // TODO: systay - this should work
}

// GetFields implements Primitive interface
func (s *Send) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not reachable")
}

func (s *Send) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Query":        s.Query,
		"Table":        s.GetTableName(),
		"NoAutoCommit": s.NoAutoCommit,
	}
	return PrimitiveDescription{
		OperatorType:      "Send",
		Keyspace:          s.Keyspace,
		TargetDestination: s.TargetDestination,
		Other:             other,
	}
}
