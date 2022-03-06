package metadata

type Shard struct {
	Nodes         []NodeInfo
	SlotRanges    []SlotRange
	ImportSlot    int
	MigratingSlot int
}

func NewShard() *Shard {
	return &Shard{
		Nodes:         make([]NodeInfo, 0),
		SlotRanges:    make([]SlotRange, 0),
		ImportSlot:    -1,
		MigratingSlot: -1,
	}
}

func (shard *Shard) HasOverlap(slotRange *SlotRange) bool {
	for _, shardSlotRange := range shard.SlotRanges {
		if shardSlotRange.HasOverlap(slotRange) {
			return true
		}
	}
	return false
}
