package dataflow

import (
	"fmt"
	"math"

	"go.uber.org/zap"
)

type DomainIdx uint64

func (d DomainIdx) Zap() zap.Field {
	return zap.Uint64("domain", uint64(d))
}

func (d DomainIdx) ZapField(field string) zap.Field {
	return zap.Uint64(field, uint64(d))
}

const InvalidDomainIdx DomainIdx = math.MaxUint64

func (dom DomainAddr) GoString() string {
	return fmt.Sprintf("[%d:%d]", dom.Domain, dom.Shard)
}
