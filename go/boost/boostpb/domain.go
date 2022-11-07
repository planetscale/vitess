package boostpb

import (
	"fmt"
	"io"
	"math"

	"go.uber.org/zap"
)

type DomainIndex uint64

func (d DomainIndex) Zap() zap.Field {
	return zap.Uint64("domain", uint64(d))
}

func (d DomainIndex) ZapField(field string) zap.Field {
	return zap.Uint64(field, uint64(d))
}

const InvalidDomainIndex DomainIndex = math.MaxUint64

type Epoch int64

func (e Epoch) Zap() zap.Field {
	return zap.Uint64("epoch", uint64(e))
}

func (desc *DomainDescriptor) Zap() zap.Field {
	return zap.String("domain", fmt.Sprintf("[%d.%d]@%s", desc.Id, desc.Shard, desc.Addr))
}

func (dom DomainAddr) LitterDump(w io.Writer) {
	fmt.Fprintf(w, "[%d:%d]", dom.Domain, dom.Shard)
}
