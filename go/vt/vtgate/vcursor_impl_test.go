package vtgate

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var _ VSchemaOperator = (*fakeVSchemaOperator)(nil)

type fakeVSchemaOperator struct {
	vschema *vindexes.VSchema
}

func (f fakeVSchemaOperator) GetCurrentSrvVschema() *vschemapb.SrvVSchema {
	panic("implement me")
}

func (f fakeVSchemaOperator) UpdateVSchema(ctx context.Context, ksName string, vschema *vschemapb.SrvVSchema) error {
	panic("implement me")
}

type fakeTopoServer struct{}

// GetTopoServer returns the full topo.Server instance.
func (f *fakeTopoServer) GetTopoServer() (*topo.Server, error) {
	return nil, nil
}

// GetSrvKeyspaceNames returns the list of keyspaces served in
// the provided cell.
func (f *fakeTopoServer) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	return []string{"ks1"}, nil
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
func (f *fakeTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	zeroHexBytes, _ := hex.DecodeString("")
	eightyHexBytes, _ := hex.DecodeString("80")
	ks := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_PRIMARY,
				ShardReferences: []*topodatapb.ShardReference{
					{Name: "-80", KeyRange: &topodatapb.KeyRange{Start: zeroHexBytes, End: eightyHexBytes}},
					{Name: "80-", KeyRange: &topodatapb.KeyRange{Start: eightyHexBytes, End: zeroHexBytes}},
				},
			},
		},
	}
	return ks, nil
}

func (f *fakeTopoServer) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	ks, err := f.GetSrvKeyspace(ctx, cell, keyspace)
	callback(ks, err)
}

// WatchSrvVSchema starts watching the SrvVSchema object for
// the provided cell.  It will call the callback when
// a new value or an error occurs.
func (f *fakeTopoServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool) {
}

func TestDestinationKeyspace(t *testing.T) {
	ks1 := &vindexes.Keyspace{
		Name:    "ks1",
		Sharded: false,
	}
	ks1Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks1,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	ks2 := &vindexes.Keyspace{
		Name:    "ks2",
		Sharded: false,
	}
	ks2Schema := &vindexes.KeyspaceSchema{
		Keyspace: ks2,
		Tables:   nil,
		Vindexes: nil,
		Error:    nil,
	}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
			ks2.Name: ks2Schema,
		},
	}

	vschemaWith1KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
		},
	}

	type testCase struct {
		vschema                 *vindexes.VSchema
		targetString, qualifier string
		expectedError           string
		expectedKeyspace        string
		expectedDest            key.Destination
		expectedTabletType      topodatapb.TabletType
	}

	tests := []testCase{{
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1:-80",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard("-80"),
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "ks1:-80@replica",
		qualifier:          "",
		expectedKeyspace:   ks1.Name,
		expectedDest:       key.DestinationShard("-80"),
		expectedTabletType: topodatapb.TabletType_REPLICA,
	}, {
		vschema:            vschemaWith1KS,
		targetString:       "",
		qualifier:          "ks1",
		expectedKeyspace:   ks1.Name,
		expectedDest:       nil,
		expectedTabletType: topodatapb.TabletType_PRIMARY,
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2",
		qualifier:     "",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "ks2:-80",
		qualifier:     "",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith1KS,
		targetString:  "",
		qualifier:     "ks2",
		expectedError: "VT05003: unknown database 'ks2' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "",
		expectedError: errNoKeyspace.Error(),
	}}

	r, _, _, _, _ := createExecutorEnv(t)
	for i, tc := range tests {
		t.Run(strconv.Itoa(i)+tc.targetString, func(t *testing.T) {
			impl, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: tc.targetString}), sqlparser.MarginComments{}, r, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, false, querypb.ExecuteOptions_Gen4)
			impl.vschema = tc.vschema
			dest, keyspace, tabletType, err := impl.TargetDestination(tc.qualifier)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedDest, dest)
				require.Equal(t, tc.expectedKeyspace, keyspace.Name)
				require.Equal(t, tc.expectedTabletType, tabletType)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

var (
	ks1            = &vindexes.Keyspace{Name: "ks1"}
	ks1Schema      = &vindexes.KeyspaceSchema{Keyspace: ks1}
	ks2            = &vindexes.Keyspace{Name: "ks2"}
	ks2Schema      = &vindexes.KeyspaceSchema{Keyspace: ks2}
	vschemaWith1KS = &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1.Name: ks1Schema,
		},
	}
)

var vschemaWith2KS = &vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		ks1.Name: ks1Schema,
		ks2.Name: ks2Schema,
	},
}

func TestSetTarget(t *testing.T) {
	type testCase struct {
		vschema       *vindexes.VSchema
		targetString  string
		expectedError string
	}

	tests := []testCase{{
		vschema:      vschemaWith2KS,
		targetString: "",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks1",
	}, {
		vschema:      vschemaWith2KS,
		targetString: "ks2",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks3",
		expectedError: "VT05003: unknown database 'ks3' in vschema",
	}, {
		vschema:       vschemaWith2KS,
		targetString:  "ks2@replica",
		expectedError: "can't execute the given command because you have an active transaction",
	}}

	r, _, _, _, _ := createExecutorEnv(t)
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			vc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{InTransaction: true}), sqlparser.MarginComments{}, r, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, nil, nil, false, querypb.ExecuteOptions_Gen4)
			vc.vschema = tc.vschema
			err := vc.SetTarget(tc.targetString)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, vc.safeSession.TargetString, tc.targetString)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestKeyForPlan(t *testing.T) {
	type testCase struct {
		vschema               *vindexes.VSchema
		targetString          string
		expectedPlanPrefixKey string
	}

	tests := []testCase{{
		vschema:               vschemaWith1KS,
		targetString:          "",
		expectedPlanPrefixKey: "ks1@primary+Collate:utf8mb4_0900_ai_ci+Query:SELECT 1",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1@replica",
		expectedPlanPrefixKey: "ks1@replica+Collate:utf8mb4_0900_ai_ci+Query:SELECT 1",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1:-80",
		expectedPlanPrefixKey: "ks1@primary+Collate:utf8mb4_0900_ai_ci+DestinationShard(-80)+Query:SELECT 1",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1[deadbeef]",
		expectedPlanPrefixKey: "ks1@primary+Collate:utf8mb4_0900_ai_ci+KsIDsResolved:80-+Query:SELECT 1",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "",
		expectedPlanPrefixKey: "ks1@primary+Collate:utf8mb4_0900_ai_ci+Query:SELECT 1",
	}, {
		vschema:               vschemaWith1KS,
		targetString:          "ks1@replica",
		expectedPlanPrefixKey: "ks1@replica+Collate:utf8mb4_0900_ai_ci+Query:SELECT 1",
	}}

	r, _, _, _, _ := createExecutorEnv(t)
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d#%s", i, tc.targetString), func(t *testing.T) {
			ss := NewSafeSession(&vtgatepb.Session{InTransaction: false})
			ss.SetTargetString(tc.targetString)
			vc, err := newVCursorImpl(ss, sqlparser.MarginComments{}, r, nil, &fakeVSchemaOperator{vschema: tc.vschema}, tc.vschema, srvtopo.NewResolver(&fakeTopoServer{}, nil, ""), nil, false, querypb.ExecuteOptions_Gen4)
			require.NoError(t, err)
			vc.vschema = tc.vschema

			var buf strings.Builder
			vc.keyForPlan(context.Background(), "SELECT 1", &buf)
			require.Equal(t, tc.expectedPlanPrefixKey, buf.String())
		})
	}
}

func TestFirstSortedKeyspace(t *testing.T) {
	ks1Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "xks1"}}
	ks2Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks2"}}
	ks3Schema := &vindexes.KeyspaceSchema{Keyspace: &vindexes.Keyspace{Name: "aks1"}}
	vschemaWith2KS := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			ks1Schema.Keyspace.Name: ks1Schema,
			ks2Schema.Keyspace.Name: ks2Schema,
			ks3Schema.Keyspace.Name: ks3Schema,
		},
	}

	r, _, _, _, _ := createExecutorEnv(t)
	vc, err := newVCursorImpl(NewSafeSession(nil), sqlparser.MarginComments{}, r, nil, &fakeVSchemaOperator{vschema: vschemaWith2KS}, vschemaWith2KS, srvtopo.NewResolver(&fakeTopoServer{}, nil, ""), nil, false, querypb.ExecuteOptions_Gen4)
	require.NoError(t, err)
	ks, err := vc.FirstSortedKeyspace()
	require.NoError(t, err)
	require.Equal(t, ks3Schema.Keyspace, ks)
}

// TestSetExecQueryTimeout tests the SetExecQueryTimeout method.
// Validates the timeout value is set based on override rule.
func TestSetExecQueryTimeout(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	safeSession := NewSafeSession(nil)
	vc, err := newVCursorImpl(safeSession, sqlparser.MarginComments{}, executor, nil, nil, &vindexes.VSchema{}, nil, nil, false, querypb.ExecuteOptions_Gen4)
	require.NoError(t, err)

	// flag timeout
	queryTimeout = 20
	vc.SetExecQueryTimeout(nil)
	require.Equal(t, 20*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 20, safeSession.Options.GetAuthoritativeTimeout())

	// session timeout
	safeSession.SetQueryTimeout(40)
	vc.SetExecQueryTimeout(nil)
	require.Equal(t, 40*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 40, safeSession.Options.GetAuthoritativeTimeout())

	// query hint timeout
	timeoutQueryHint := 60
	vc.SetExecQueryTimeout(&timeoutQueryHint)
	require.Equal(t, 60*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 60, safeSession.Options.GetAuthoritativeTimeout())

	// query hint timeout - infinite
	timeoutQueryHint = 0
	vc.SetExecQueryTimeout(&timeoutQueryHint)
	require.Equal(t, 0*time.Millisecond, vc.queryTimeout)
	require.NotNil(t, safeSession.Options.Timeout)
	require.EqualValues(t, 0, safeSession.Options.GetAuthoritativeTimeout())

	// reset
	queryTimeout = 0
	safeSession.SetQueryTimeout(0)
	vc.SetExecQueryTimeout(nil)
	require.Equal(t, 0*time.Millisecond, vc.queryTimeout)
	// this should be reset.
	require.Nil(t, safeSession.Options.Timeout)
}

func TestRecordMirrorStats(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	safeSession := NewSafeSession(nil)
	logStats := logstats.NewLogStats(context.Background(), t.Name(), "select 1", "", nil)
	vc, err := newVCursorImpl(safeSession, sqlparser.MarginComments{}, executor, logStats, nil, &vindexes.VSchema{}, nil, nil, false, querypb.ExecuteOptions_Gen4)
	require.NoError(t, err)

	require.Zero(t, logStats.MirrorSourceExecuteTime)
	require.Zero(t, logStats.MirrorTargetExecuteTime)
	require.Nil(t, logStats.MirrorTargetError)

	vc.RecordMirrorStats(10*time.Millisecond, 20*time.Millisecond, errors.New("test error"))

	require.Equal(t, 10*time.Millisecond, logStats.MirrorSourceExecuteTime)
	require.Equal(t, 20*time.Millisecond, logStats.MirrorTargetExecuteTime)
	require.ErrorContains(t, logStats.MirrorTargetError, "test error")
}
