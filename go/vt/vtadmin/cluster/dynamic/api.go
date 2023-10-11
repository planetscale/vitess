package dynamic

import (
	"net/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
)

// API is the interface dynamic APIs must implement.
// It is implemented by vtadmin.API.
type API interface {
	vtadminpb.VTAdminServer
	WithCluster(c *cluster.Cluster, id string) API
	Handler() http.Handler
}
