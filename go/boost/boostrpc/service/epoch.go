package service

import "go.uber.org/zap"

type Epoch int64

func (e Epoch) Zap() zap.Field {
	return zap.Uint64("epoch", uint64(e))
}
