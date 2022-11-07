package server

import (
	"errors"

	"storj.io/drpc"
	"storj.io/drpc/drpcmetadata"
)

// TokenValidator represents an external authentication mechanism used to validate
// bearer credentials. In case of any error the validation function must return
// 'false' and the server will return an 'invalid credentials' error message.
type TokenValidator func(token string) bool

// AuthByToken allows to use an external authentication mechanism using bearer
// tokens as credentials. The token must be present in the request's metadata
// under `key` and valid according to the provided `validator`.
func AuthByToken(key string, validator TokenValidator) Middleware {
	return func(next drpc.Handler) drpc.Handler {
		return authToken{
			key:  key,
			val:  validator,
			next: next,
		}
	}
}

type authToken struct {
	key  string
	val  TokenValidator
	next drpc.Handler
}

func (md authToken) HandleRPC(stream drpc.Stream, rpc string) (err error) {
	data, ok := drpcmetadata.Get(stream.Context())
	if !ok {
		return errors.New("authentication: missing credentials")
	}
	token, ok := data[md.key]
	if !ok {
		return errors.New("authentication: missing credentials")
	}
	if !md.val(token) {
		return errors.New("authentication: invalid credentials")
	}
	return md.next.HandleRPC(stream, rpc)
}
