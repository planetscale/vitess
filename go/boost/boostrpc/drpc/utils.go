package drpc

import (
	"context"
	"os"

	"storj.io/drpc/drpcmetadata"
)

// Helper method to determine if a path exists and is a regular file.
func exists(name string) bool {
	info, err := os.Stat(name)
	return err == nil && !info.IsDir()
}

// ContextWithMetadata adds custom data to a given context. This is particularly
// useful when sending outgoing requests on the client side.
func ContextWithMetadata(ctx context.Context, data map[string]string) context.Context {
	return drpcmetadata.AddPairs(ctx, data)
}

// MetadataFromContext retrieve any custom metadata available on the provided
// context. This is particularly useful when processing incoming requests on
// the server side.
func MetadataFromContext(ctx context.Context) (map[string]string, bool) {
	return drpcmetadata.Get(ctx)
}
