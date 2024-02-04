package vindexes

import (
	"context"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	_                    IMultiTenant = (*MultiTenant)(nil)
	TenantMigrationCache              = newTenantMigrationCache()
)

type tenantMigrationCache struct {
	mu    sync.Mutex
	cache map[int64]vschemapb.TenantStatus
}

func newTenantMigrationCache() *tenantMigrationCache {
	return &tenantMigrationCache{
		cache: make(map[int64]vschemapb.TenantStatus),
	}
}

func (t *tenantMigrationCache) Get(tenantId int64) vschemapb.TenantStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, ok := t.cache[tenantId]
	if !ok {
		return vschemapb.TenantStatus_UNKNOWN
	}
	return status
}

func (t *tenantMigrationCache) Init(migrations *vschemapb.TenantMigrations) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, migration := range migrations.Statuses {
		t.cache[migration.TenantId] = migration.Status
	}
}

func init() {
	Register("multitenant", newMultiTenant)
	newTenantMigrationCache()
}

func newMultiTenant(name string, m map[string]string) (Vindex, error) {
	mc, err := newMultiCol(name, m)
	if err != nil {
		return nil, err
	}
	mt := &MultiTenant{
		MultiCol: mc.(*MultiCol),
	}
	var ok bool
	mt.tenantIdColumnName, ok = m["tenant_id_column_name"]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "tenant_id_column_name must be specified for multitenant vindex")
	}
	return mt, nil
}

type MultiTenant struct {
	*MultiCol
	tenantIdColumnName string
}

func (m MultiTenant) String() string {
	return m.MultiCol.String()
}

func (m MultiTenant) Cost() int {
	return m.MultiCol.Cost()
}

func (m MultiTenant) IsUnique() bool {
	return m.MultiCol.IsUnique()
}

func (m MultiTenant) NeedsVCursor() bool {
	return m.MultiCol.NeedsVCursor()
}

// cannot rewrite keyspace here
func (m MultiTenant) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	for _, rowColValues := range rowsColValues {
		tenantId, err := rowColValues[0].ToInt64()
		if err != nil {
			return nil, err
		}
		status := TenantMigrationCache.Get(tenantId)
		log.Infof("abcxz Map tenant id is %v, status %v", tenantId, status)
		if status != vschemapb.TenantStatus_MIGRATED {
			log.Infof("abcxz tenant %d is not migrated", tenantId)
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tenant %d is not migrated", tenantId)
		}
	}
	return m.MultiCol.Map(ctx, vcursor, rowsColValues)
}

func (m MultiTenant) Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return m.MultiCol.Verify(ctx, vcursor, rowsColValues, ksids)
}

func (m MultiTenant) PartialVindex() bool {
	return m.MultiCol.PartialVindex()
}
