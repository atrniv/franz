package domain

import (
	"sync"

	uuid "github.com/satori/go.uuid"
)

type Cluster struct {
	ID      string
	brokers []*Broker
	topics  []*Topic
	sync.RWMutex
}

func (c *Cluster) StartBroker(addr string, expectedMembers map[string]int, waitForConsumers bool) error {
	broker := NewBroker(c)
	c.brokers = append(c.brokers, broker)
	return broker.Start(addr, expectedMembers, waitForConsumers)
}

func (c *Cluster) GetBrokers() []*Broker {
	c.RLock()
	defer c.RUnlock()
	return c.brokers
}

func (c *Cluster) GetTopic(topicName string) (*Topic, error) {
	c.RLock()
	defer c.RUnlock()
	for _, topic := range c.topics {
		if topic.Name == topicName {
			return topic, nil
		}
	}
	return nil, ErrUnknownTopicOrPartition
}

func (c *Cluster) GetTopics() []*Topic {
	c.RLock()
	defer c.RUnlock()
	return c.topics
}

func (c *Cluster) ReceiveMessages(topicName string, partitionIndex int32, offset int64, currentSize int, maxSize int) ([]Message, int, error) {
	topic, err := c.GetTopic(topicName)
	if err != nil {
		return nil, 0, err
	}
	partition, err := topic.GetPartition(partitionIndex)
	if err != nil {
		return nil, 0, err
	}
	messages := partition.FetchRawMessages(offset, maxSize-currentSize)
	return messages, len(messages), nil
}

type TopicStats struct {
	Name            string `json:"name"`
	MessageReceived int64  `json:"messages_received"`
	LastReceivedAt  int64  `json:"last_received_at"`
}

func (c *Cluster) TopicStats(topicNames []string) ([]TopicStats, error) {
	c.RLock()
	defer c.RUnlock()
	stats := []TopicStats{}
	for _, topicName := range topicNames {
		topic, err := c.GetTopic(topicName)
		if err != nil {
			return nil, err
		}
		partitionCount := topic.PartitionCount()
		messagesReceived := int64(0)
		lastReceivedAt := int64(0)
		for index := 0; index < partitionCount; index++ {
			partition, err := topic.GetPartition(int32(index))
			if err != nil {
				return nil, err
			}

			partitionMessagesReceived, partitionLastReceivedAt := partition.GetStats()
			messagesReceived += partitionMessagesReceived
			if partitionLastReceivedAt > lastReceivedAt {
				lastReceivedAt = partitionLastReceivedAt
			}

		}
		stats = append(stats, TopicStats{
			Name:            topicName,
			MessageReceived: messagesReceived,
			LastReceivedAt:  lastReceivedAt,
		})
	}
	return stats, nil
}

func (c *Cluster) Lag(groupIDs []string, topicNames []string) int64 {
	c.RLock()
	defer c.RUnlock()
	lag := int64(0)
	offsets := map[string]map[int32]int64{}
	for _, topic := range c.topics {
		exists := false
		for _, topicName := range topicNames {
			if topicName == topic.Name {
				exists = true
				break
			}
		}
		if !exists {
			continue
		}
		topicOffsets := map[int32]int64{}
		for _, partition := range topic.Partitions {
			nextOffset, _ := partition.GetOffset(-1)
			lastOffset := nextOffset - 1
			topicOffsets[partition.Index] = lastOffset
		}
		offsets[topic.Name] = topicOffsets
	}
	for _, broker := range c.brokers {
		lag += broker.Lag(groupIDs, offsets)
	}
	return lag
}

func (c *Cluster) CreateTopic(name string, partitions int) (*Topic, error) {
	c.Lock()
	defer c.Unlock()
	for _, topic := range c.topics {
		if topic.Name == name {
			return nil, ErrTopicAlreadyExists
		}
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]*Partition, partitions),
	}

	leader := c.brokers[0]

	for index := range topic.Partitions {
		topic.Partitions[index] = &Partition{
			Index:         int32(index),
			Topic:         topic,
			LeaderID:      leader.ID,
			ISRNodes:      []int32{leader.ID},
			ReplicaNodes:  []int32{leader.ID},
			LeaderEpoch:   0,
			LastOffset:    -1,
			LastTimestamp: -1,
			Messages:      []Message{},
		}
	}

	c.topics = append(c.topics, topic)
	return topic, nil
}

func (c *Cluster) Reset() {
	c.Lock()
	for _, topic := range c.topics {
		topic.Reset()
	}
	for _, broker := range c.brokers {
		broker.Reset()
	}
	c.Unlock()
}

func (c *Cluster) Stop() error {
	for _, broker := range c.brokers {
		err := broker.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}

func NewCluster(id string) *Cluster {
	return &Cluster{
		ID:      id + uuid.NewV1().String(),
		brokers: []*Broker{},
		topics:  []*Topic{},
	}
}
