package cli

import (
	boostclient "vitess.io/vitess/go/boost/topo/client"
)

// PlanetScale internal: used by plug-ins that interact with boost
var boost *boostclient.Client
