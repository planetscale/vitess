#!/usr/bin/env bash

set -ex

if [[ -z "$VTROOT" ]]; then
  # shellcheck disable=SC2016
  echo '$VTROOT is not set; have you sourced dev.env?'
  exit 1
fi

export GOBIN="$VTROOT/bin"

go install ./proto/cmd/protoc-gen-go-boost
go install storj.io/drpc/cmd/protoc-gen-go-drpc@$(go list -m -f '{{ .Version }}' storj.io/drpc)

for pb in ./proto/*.proto; do
  protoc \
    --go-boost_out=. --plugin protoc-gen-go-boost="${GOBIN}/protoc-gen-go-boost" \
    --go-drpc_out=. --plugin protoc-gen-go-drpc="${GOBIN}/protoc-gen-go-drpc" \
    --go-drpc_opt=protolib=vitess.io/vitess/go/boost/boostrpc/codec \
    -I "$VTROOT/proto" -I ./proto "$pb"
done

cp -Rf ./vitess.io/vitess/go/boost/* .
rm -rf ./vitess.io
