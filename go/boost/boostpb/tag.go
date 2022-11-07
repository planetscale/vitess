package boostpb

import (
	"math"

	"go.uber.org/zap"
)

type Tag uint32

const TagInvalid Tag = math.MaxUint32

type MaterializationStatus byte

const (
	MaterializationNone MaterializationStatus = iota
	MaterializationFull
	MaterializationPartial
)

func (t Tag) Zap() zap.Field {
	return zap.Uint32("tag", uint32(t))
}
