package packet

type AsyncPacket interface {
	isAsyncPacket()
}

func (*Input) isAsyncPacket()                {}
func (*Message) isAsyncPacket()              {}
func (*ReplayPiece) isAsyncPacket()          {}
func (*EvictRequest) isAsyncPacket()         {}
func (*EvictKeysRequest) isAsyncPacket()     {}
func (*FinishReplayRequest) isAsyncPacket()  {}
func (*AddNodeRequest) isAsyncPacket()       {}
func (*RemoveNodesRequest) isAsyncPacket()   {}
func (*UpdateEgressRequest) isAsyncPacket()  {}
func (*UpdateSharderRequest) isAsyncPacket() {}
func (*PrepareStateRequest) isAsyncPacket()  {}
func (*PartialReplayRequest) isAsyncPacket() {}
func (*ReaderReplayRequest) isAsyncPacket()  {}
func (*StartReplayRequest) isAsyncPacket()   {}

type SyncPacket interface {
	isSyncPacket()
}

func (*ReadyRequest) isSyncPacket()           {}
func (*SetupReplayPathRequest) isSyncPacket() {}

type ReplayContext = isReplayPiece_Context
