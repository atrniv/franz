package domain

import (
	"sync"

	"github.com/atrniv/franz/protocol"
)

type Topic struct {
	Name          string
	TimestampType protocol.TimestampType
	Partitions    []*Partition
	sync.RWMutex
}

func (t *Topic) PartitionCount() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.Partitions)
}

func (t *Topic) GetPartition(partitionIndex int32) (*Partition, error) {
	t.RLock()
	defer t.RUnlock()
	for index, partition := range t.Partitions {
		if index == int(partitionIndex) {
			return partition, nil
		}
	}
	return nil, ErrUnknownTopicOrPartition
}

func (t *Topic) Reset() {
	t.Lock()
	for _, partition := range t.Partitions {
		partition.Reset()
	}
	t.Unlock()
}
