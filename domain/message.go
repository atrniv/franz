package domain

import (
	"github.com/atrniv/franz/protocol"
)

type Message struct {
	Timestamp int64
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

type Header struct {
	Key   []byte
	Value []byte
}

func NewMessagesFromRecordBatch(batch protocol.RecordBatch, appendTime int64, lastOffset int64) ([]Message, int64) {
	messages := []Message{}
	for _, record := range batch.Records {
		lastOffset++
		timestamp := batch.FirstTimestamp + record.TimestampDelta
		if batch.Attributes.TimestampType() == protocol.LogAppendTime {
			timestamp = appendTime
		} else {
			appendTime = -1
		}
		messages = append(messages, Message{
			Headers:   newHeaders(record.Headers),
			Offset:    lastOffset,
			Timestamp: timestamp,
			Key:       record.Key,
			Value:     record.Value,
		})
	}
	return messages, appendTime
}

func NewMessagesFromMessageSet(set protocol.MessageSet, appendTime int64, lastOffset int64) ([]Message, int64) {
	messages := []Message{}
	for _, block := range set {
		lastOffset++
		timestamp := block.Message.Timestamp
		if block.Message.Attributes.TimestampType() == protocol.LogAppendTime || block.Message.Timestamp == 0 {
			timestamp = appendTime
		} else {
			appendTime = -1
		}
		messages = append(messages, Message{
			Headers:   []Header{},
			Offset:    lastOffset,
			Timestamp: timestamp,
			Key:       block.Message.Key,
			Value:     block.Message.Value,
		})
	}
	return messages, appendTime
}

func NewRecordBatchFromMessages(messages []Message, compressionCodec protocol.CompressionCodec, compressionLevel int, timestampType protocol.TimestampType) (protocol.RecordBatch, error) {
	batch := protocol.RecordBatch{}
	batch.FirstOffset = messages[0].Offset
	batch.FirstTimestamp = messages[0].Timestamp
	batch.CompressionLevel = compressionLevel
	batch.Attributes = protocol.NewRecordBatchAttribute(compressionCodec, false, timestampType, false)
	batch.Magic = 2
	batch.MaxTimestamp = messages[len(messages)-1].Timestamp
	batch.LastOffsetDelta = int32(messages[len(messages)-1].Offset - batch.FirstOffset)

	records := []protocol.Record{}
	for _, message := range messages {
		records = append(records, protocol.Record{
			TimestampDelta: message.Timestamp - batch.FirstTimestamp,
			OffsetDelta:    message.Offset - batch.FirstOffset,
			Key:            message.Key,
			Value:          message.Value,
			Headers:        newProtocolHeaders(message.Headers),
		})
	}

	batch.Records = records
	return batch, nil
}

func NewMessageSetFromMessages(messages []Message, messageVersion int8, compressionCodec protocol.CompressionCodec, compressionLevel int, timestampType protocol.TimestampType) (protocol.MessageSet, error) {
	set := make(protocol.MessageSet, 1)

	compressedMessages := protocol.MessageSet{}
	for index, message := range messages {
		compressedMessages = append(compressedMessages, protocol.MessageBlock{
			Offset: int64(index),
			Message: protocol.Message{
				MagicByte:        messageVersion,
				CompressionLevel: compressionLevel,
				Attributes:       protocol.NewMessageAttributes(protocol.CompressionNone, timestampType),
				Timestamp:        message.Timestamp,
				Key:              message.Key,
				Value:            message.Value,
			},
		})
	}
	messageBuffer := protocol.NewWriter(nil)
	compressedMessages.Write(messageVersion, messageBuffer)
	messageData, err := messageBuffer.Data()
	if err != nil {
		return set, err
	}

	set[0] = protocol.MessageBlock{
		Offset: messages[0].Offset,
		Message: protocol.Message{
			Attributes: protocol.NewMessageAttributes(compressionCodec, timestampType),
			Key:        nil,
			Value:      messageData,
		},
	}

	return set, nil
}

func newHeaders(headers []protocol.Header) []Header {
	data := make([]Header, len(headers))
	for index, header := range headers {
		data[index] = Header{
			Key:   header.Key,
			Value: header.Value,
		}
	}
	return data
}

func newProtocolHeaders(headers []Header) []protocol.Header {
	data := make([]protocol.Header, len(headers))
	for index, header := range headers {
		data[index] = protocol.Header{
			Key:   header.Key,
			Value: header.Value,
		}
	}
	return data
}
