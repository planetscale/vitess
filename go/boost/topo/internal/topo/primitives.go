package topo

import (
	"context"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"
)

func Load[T any, M interface {
	*T
	proto.Message
}](ctx context.Context, ts *topo.Server, path string) (M, error) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return nil, err
	}

	data, _, err := conn.Get(ctx, path)
	if err != nil {
		return nil, err
	}

	var state T
	if err = proto.Unmarshal(data, M(&state)); err != nil {
		return nil, err
	}
	return &state, nil
}

func Store[M proto.Message](ctx context.Context, ts *topo.Server, path string, value M) error {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}
	serialized, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	_, err = conn.Update(ctx, path, serialized, nil)
	return err
}

func Update[T any, M interface {
	*T
	proto.Message
}](ctx context.Context, ts *topo.Server, path string, update func(msg M) error) (M, error) {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return nil, err
	}
	for {
		var state T
		created := false
		data, version, err := conn.Get(ctx, path)
		if err != nil {
			if !topo.IsErrType(err, topo.NoNode) {
				return nil, err
			}
			created = true
		} else {
			if err := proto.Unmarshal(data, M(&state)); err != nil {
				return nil, err
			}
		}
		if err := update(&state); err != nil {
			return nil, err
		}
		serialized, err := proto.Marshal(M(&state))
		if err != nil {
			return nil, err
		}

		// If we fail any of the last steps to write the value,
		// we let the loop retry. We only return if we are successful
		// to redo checks, validations etc. to run the given callback.
		if created {
			if _, err := conn.Create(ctx, path, serialized); err == nil {
				return &state, nil
			}
		} else {
			if _, err := conn.Update(ctx, path, serialized, version); err == nil {
				return &state, nil
			}
		}
	}
}

// Purge recursively walks the topo starting at the boost base path and deletes
// all data.
func Purge(ctx context.Context, ts *topo.Server) error {
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	kvs, err := conn.List(ctx, pathBase)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			// Nothing to do.
			return nil
		}

		return err
	}

	for _, kv := range kvs {
		err := conn.Delete(ctx, string(kv.Key), nil)
		if err != nil && !topo.IsErrType(err, topo.NoNode) {
			return err
		}
	}

	return nil
}
