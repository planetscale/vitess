package topotools

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

func GetTenantMigrations(ctx context.Context, ts *topo.Server) (map[int64]vschemapb.TenantStatus, error) {
	tmss, err := ts.GetTenantMigrations(ctx)
	if err != nil {
		return nil, err
	}
	statuses := GetTenantMigrationMap(tmss)
	return statuses, nil
}

func GetTenantMigrationMap(tmss *vschemapb.TenantMigrations) map[int64]vschemapb.TenantStatus {
	if tmss == nil {
		return nil
	}
	statuses := make(map[int64]vschemapb.TenantStatus, len(tmss.Statuses))
	for _, tms := range tmss.Statuses {
		statuses[tms.TenantId] = tms.Status
	}
	return statuses
}

func SaveTenantMigrations(ctx context.Context, ts *topo.Server, statuses map[int64]vschemapb.TenantStatus) error {
	log.Infof("abcxz Saving tenant migrations:%d: %v\n", len(statuses), statuses)
	tmss := &vschemapb.TenantMigrations{Statuses: make([]*vschemapb.TenantMigration, 0, len(statuses))}
	for tenantId, status := range statuses {
		log.Infof("abcxz Adding tenant migration %v: %v\n", tenantId, status)
		tmss.Statuses = append(tmss.Statuses, &vschemapb.TenantMigration{
			TenantId: tenantId,
			Status:   status,
		})
	}
	return ts.SaveTenantMigrations(ctx, tmss)
}
