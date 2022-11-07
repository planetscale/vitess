package boostpb

import "fmt"

func NewShardingForcedNone() Sharding {
	return Sharding{Mode: Sharding_ForcedNone}
}

func NewShardingNone() Sharding {
	return Sharding{Mode: Sharding_None}
}

func NewShardingByColumn(col int, shards uint) Sharding {
	return Sharding{
		Mode:   Sharding_ByColumn,
		Col:    col,
		Shards: shards,
	}
}

func NewShardingRandom(shards uint) Sharding {
	return Sharding{
		Mode:   Sharding_Random,
		Shards: shards,
	}
}

func (s Sharding) ByColumn() (col int, shard uint, ok bool) {
	if s.Mode == Sharding_ByColumn {
		return s.Col, s.Shards, true
	}
	return 0, 0, false
}

func (s Sharding) IsNone() bool {
	return s.Mode == Sharding_None || s.Mode == Sharding_ForcedNone
}

func (s Sharding) TryGetShards() *uint {
	switch s.Mode {
	case Sharding_ByColumn, Sharding_Random:
		return &s.Shards
	default:
		return nil
	}
}

func (s Sharding) GetShards() uint {
	switch s.Mode {
	case Sharding_ByColumn, Sharding_Random:
		return s.Shards
	default:
		return 1
	}
}

func (s Sharding) DebugString() string {
	switch s.Mode {
	case Sharding_None:
		return "Sharding::None"
	case Sharding_ForcedNone:
		return "Sharding::ForcedNone"
	case Sharding_Random:
		return fmt.Sprintf("Sharding::Random(shards=%d)", s.Shards)
	case Sharding_ByColumn:
		return fmt.Sprintf("Sharding::ByColumn(col=%d, shards=%d)", s.Col, s.Shards)
	}
	return ""
}
