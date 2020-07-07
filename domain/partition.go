package domain

import (
	"sync"
	"time"

	"github.com/atrniv/franz/protocol"
)

type Partition struct {
	Index         int32
	Topic         *Topic
	Messages      []Message
	LastOffset    int64
	LastTimestamp int64
	LeaderID      int32
	LeaderEpoch   int32
	ReplicaNodes  []int32
	ISRNodes      []int32
	sync.RWMutex
}

func (p *Partition) GetOffset(timestamp int64) (int64, int64) {
	p.RLock()
	defer p.RUnlock()
	// Return next offset to fetch
	if timestamp == -1 {
		if len(p.Messages) == 0 {
			if p.LastOffset >= 0 {
				return p.LastOffset + 1, p.LastTimestamp
			}
			return 0, 0
		}

		message := p.Messages[len(p.Messages)-1]
		return message.Offset + 1, message.Timestamp

		// Return earliest offset to fetch
	} else if timestamp == -2 {
		if len(p.Messages) == 0 {
			if p.LastOffset >= 0 {
				return p.LastOffset + 1, p.LastTimestamp
			}
			return 0, 0
		}
		message := p.Messages[0]
		return message.Offset, message.Timestamp
		// Return earliest timestamp to fetch
	} else {
		if len(p.Messages) == 0 {
			if p.LastOffset >= 0 {
				return p.LastOffset + 1, p.LastTimestamp
			}
			return 0, 0
		}
		lastMessage := p.Messages[0]
		for _, message := range p.Messages {
			if message.Timestamp > timestamp {
				break
			}
		}
		return lastMessage.Offset, lastMessage.Timestamp
	}
}

func (p *Partition) GetStats() (int64, int64) {
	p.RLock()
	defer p.RUnlock()
	if len(p.Messages) > 0 {
		return int64(len(p.Messages)), p.Messages[len(p.Messages)-1].Timestamp
	}
	return 0, 0
}

func (p *Partition) AppendMessages(apiVersion int16, records protocol.RecordData) (int64, int64, int64, error) {
	p.Lock()
	defer p.Unlock()
	timestamp := time.Now().UnixNano() / 1e6
	lastOffset := int64(-1)
	if len(p.Messages) > 0 {
		lastOffset = p.Messages[len(p.Messages)-1].Offset
	}
	var messages []Message
	if apiVersion >= 3 {
		messages, timestamp = NewMessagesFromRecordBatch(records.RecordBatch, timestamp, lastOffset)
		p.Messages = append(p.Messages, messages...)
	} else {
		messages, timestamp = NewMessagesFromMessageSet(records.MessageSet, timestamp, lastOffset)
		p.Messages = append(p.Messages, messages...)
	}
	firstOffset := p.Messages[0].Offset
	return messages[0].Offset, timestamp, firstOffset, nil
}

func (p *Partition) FetchMessages(apiVersion int16, offset int64, totalSize int32, logStartOffset int64, leaderEpoch int32, maxBytes int32, expiresAt time.Time) ([]byte, int, int32, int64, int64, int64, error) {
	messages := []Message{}
	p.RLock()
	if offset == -1 {
		if len(messages) > 0 {
			offset = p.Messages[len(p.Messages)-1].Offset + 1
		}
	} else if offset == -2 {
		if len(messages) > 0 {
			offset = p.Messages[0].Offset
		}
	}

	for _, message := range p.Messages {
		if message.Offset >= offset {
			messages = append(messages, message)
			totalSize += int32(len(message.Key) + len(message.Value))
		}
		if totalSize > maxBytes {
			break
		}
	}
	actualLogStartOffset := int64(-1)
	highWatermark := int64(-1)
	lastStableOffset := int64(-1)
	if len(p.Messages) > 0 {
		actualLogStartOffset = p.Messages[0].Offset
		lastStableOffset = p.Messages[len(p.Messages)-1].Offset
		highWatermark = p.Messages[len(p.Messages)-1].Offset
	} else {
		if p.LastOffset > 0 {
			actualLogStartOffset = p.LastOffset + 1
			highWatermark = p.LastOffset + 1
			lastStableOffset = p.LastOffset
		}
	}
	p.RUnlock()

	if len(messages) > 0 {
		var err error
		writer := protocol.NewWriter(nil)
		recordData := protocol.RecordData{}
		if apiVersion >= 4 {
			recordData.Version = 2
			recordData.RecordBatch, err = NewRecordBatchFromMessages(messages, protocol.CompressionSnappy, protocol.CompressionLevelDefault, p.Topic.TimestampType)
			if err != nil {
				return nil, 0, 0, 0, 0, 0, err
			}
		} else if apiVersion >= 2 {
			recordData.Version = 1
			recordData.MessageSet, err = NewMessageSetFromMessages(messages, 1, protocol.CompressionSnappy, protocol.CompressionLevelDefault, p.Topic.TimestampType)
			if err != nil {
				return nil, 0, 0, 0, 0, 0, err
			}
		} else {
			recordData.Version = 0
			recordData.MessageSet, err = NewMessageSetFromMessages(messages, 0, protocol.CompressionSnappy, protocol.CompressionLevelDefault, p.Topic.TimestampType)
			if err != nil {
				return nil, 0, 0, 0, 0, 0, err
			}
		}
		recordData.Write(writer)
		data, err := writer.Data()
		if err != nil {
			return nil, 0, 0, 0, 0, 0, err
		}

		return data, len(messages), totalSize, actualLogStartOffset, lastStableOffset, highWatermark, nil
	}
	return []byte{}, len(messages), totalSize, actualLogStartOffset, lastStableOffset, highWatermark, nil
}

func (p *Partition) AppendRawMessages(messages []Message) {
	p.Lock()
	defer p.Unlock()
	timestamp := time.Now().UnixNano() / 1e6
	lastOffset := p.LastOffset
	if len(p.Messages) > 0 {
		lastOffset = p.Messages[len(p.Messages)-1].Offset
	}
	for index, message := range messages {
		lastOffset++
		message.Offset = lastOffset
		message.Timestamp = timestamp
		messages[index] = message
	}
	p.Messages = append(p.Messages, messages...)
}

func (p *Partition) FetchRawMessages(offset int64, maxMessages int) []Message {
	p.RLock()
	defer p.RUnlock()
	messages := []Message{}
	for _, message := range p.Messages {
		if message.Offset >= offset {
			messages = append(messages, message)
		}
		if maxMessages > 0 && len(messages) >= maxMessages {
			break
		}
	}
	return messages
}

func (p *Partition) Reset() {
	p.Lock()
	if len(p.Messages) > 0 {
		p.LastOffset = p.Messages[len(p.Messages)-1].Offset
		p.LastTimestamp = p.Messages[len(p.Messages)-1].Timestamp
		p.Messages = []Message{}
	}
	// p.LeaderID = 0
	// p.LeaderEpoch = 0
	p.Unlock()
}
