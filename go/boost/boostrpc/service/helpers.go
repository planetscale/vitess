package service

import (
	"fmt"

	"go.uber.org/zap"
)

func (desc *DomainDescriptor) Zap() zap.Field {
	return zap.String("domain", fmt.Sprintf("[%d.%d]@%s", desc.Id, desc.Shard, desc.Addr))
}
