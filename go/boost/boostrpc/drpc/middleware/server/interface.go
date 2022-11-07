package server

import (
	"storj.io/drpc"
)

// Middleware elements allow to customize the internal requests processing
// by the server.
type Middleware func(drpc.Handler) drpc.Handler
