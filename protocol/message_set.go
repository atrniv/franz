package protocol

/*

Changelog
---------
- Timestamp added , KIP-32

*/

type MessageSet []MessageBlock

type MessageBlock struct {
	Offset      int64
	MessageSize int32
	Message     Message
}

func (r MessageSet) Write(messageVersion int8, w *Writer) {
	for _, block := range r {
		w.Int64(block.Offset)
		mw := NewWriter(nil)
		block.Message.Write(messageVersion, mw)
		md, err := mw.Data()
		if err != nil {
			w.err = err
			return
		}
		w.Int32(int32(len(md)))
		w.RawBytes(md)
	}
}

func ReadMessageSet(messageVersion int8, r *Reader) MessageSet {
	set := MessageSet{}
	for r.pos < len(r.data) {
		block := MessageBlock{}
		block.Offset = r.Int64()
		block.MessageSize = r.Int32()
		block.Message = ReadMessage(messageVersion, r)
		set = append(set, block)
	}
	if len(set) == 1 {
		block := set[0]
		if block.Message.Attributes.CompressionCodec() != CompressionNone {
			data := r.Decompress(block.Message.Attributes.CompressionCodec(), block.Message.Value)
			messages := ReadMessageSet(messageVersion, NewReader(data))
			for index, message := range messages {
				message.Offset = block.Offset + int64(index)
			}
			return messages
		}
	}
	return set
}

type Message struct {
	CRC              int32
	MagicByte        int8
	CompressionLevel int
	Attributes       MessageAttributes
	Timestamp        int64
	Key              []byte
	Value            []byte
}

func (r Message) Write(messageVersion int8, w *Writer) {
	//TODO: Create the correct CRC

	mw := NewWriter(nil)

	mw.Int8(r.MagicByte)
	mw.Int8(int8(r.Attributes))

	if messageVersion >= 1 {
		mw.Int64(r.Timestamp)
	}

	mw.NullableBytes(r.Key)

	if r.Attributes.CompressionCodec() != CompressionNone {
		mw.NullableBytes(w.Compress(r.Attributes.CompressionCodec(), r.CompressionLevel, r.Value))
	} else {
		mw.NullableBytes(r.Value)
	}

	data, err := mw.Data()
	if err != nil {
		w.err = err
		return
	}

	// Calculate CRC
	w.CRC32IEEE(data)
	w.RawBytes(data)
}

func ReadMessage(messageVersion int8, r *Reader) Message {
	message := Message{}
	message.CRC = r.Int32()
	message.MagicByte = r.Int8()
	message.Attributes = MessageAttributes(r.Int8())
	if message.MagicByte >= 1 {
		message.Timestamp = r.Int64()
	}
	message.Key = r.NullableBytes()
	message.Value = r.NullableBytes()
	return message
}

type MessageAttributes int8

func (a MessageAttributes) CompressionCodec() CompressionCodec {
	return CompressionCodec(int8(a) & compressionCodecMask)
}
func (a MessageAttributes) TimestampType() TimestampType {
	return TimestampType(int8(a) & timestampTypeMask)
}

func NewMessageAttributes(codec CompressionCodec, timestampType TimestampType) MessageAttributes {
	attributes := int8(codec) & compressionCodecMask
	if timestampType == LogAppendTime {
		attributes |= timestampTypeMask
	}
	return MessageAttributes(attributes)
}
