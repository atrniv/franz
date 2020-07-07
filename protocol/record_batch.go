package protocol

/*
Changelog
---------
- PartitionLeaderEpoch added in KIP-101
- ProducerID, ProducerEpoch FirstSequence added in KIP-98


RecordBatch =>
  FirstOffset => int64
  Length => int32
  PartitionLeaderEpoch => int32
  Magic => int8
  CRC => int32
  Attributes => int16
  LastOffsetDelta => int32
  FirstTimestamp => int64
  MaxTimestamp => int64
  ProducerId => int64
  ProducerEpoch => int16
  FirstSequence => int32
  Records => [Record]
*/
type RecordBatch struct {
	CompressionLevel     int
	FirstOffset          int64
	Length               int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           RecordBatchAttributes
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	FirstSequence        int32
	Records              []Record
}

func (r RecordBatch) Write(messageVersion int8, w *Writer) {

	bw := NewWriter(nil)
	bw.Int16(int16(r.Attributes))
	bw.Int32(r.LastOffsetDelta)
	bw.Int64(r.FirstTimestamp)
	bw.Int64(r.MaxTimestamp)
	bw.Int64(r.ProducerID)
	bw.Int16(r.ProducerEpoch)
	bw.Int32(r.FirstSequence)

	bw.Int32(int32(len(r.Records)))

	rw := NewWriter(nil)
	for _, record := range r.Records {
		record.Write(messageVersion, rw)
	}
	recordData, err := rw.Data()
	if err != nil {
		w.err = err
		return
	}
	compressedRecordData := bw.Compress(r.Attributes.CompressionCodec(), r.CompressionLevel, recordData)
	bw.RawBytes(compressedRecordData)

	data, err := bw.Data()
	if err != nil {
		w.err = err
		return
	}

	w.Int64(r.FirstOffset)
	w.Int32(int32(len(compressedRecordData) + recordBatchOverhead))
	w.Int32(r.PartitionLeaderEpoch)
	w.Int8(r.Magic)
	w.CRC32Castagnoli(data)
	w.RawBytes(data)
}

func ReadRecordBatch(messageVersion int8, r *Reader) RecordBatch {
	batch := RecordBatch{}
	batch.FirstOffset = r.Int64()
	batch.Length = r.Int32()
	batch.PartitionLeaderEpoch = r.Int32()
	batch.Magic = r.Int8()
	batch.CRC = r.Uint32()
	batch.Attributes = RecordBatchAttributes(r.Int16())
	batch.LastOffsetDelta = r.Int32()
	batch.FirstTimestamp = r.Int64()
	batch.MaxTimestamp = r.Int64()
	batch.ProducerID = r.Int64()
	batch.ProducerEpoch = r.Int16()
	batch.FirstSequence = r.Int32()
	batch.Records = make([]Record, r.Int32())
	if batch.Length > 0 {
		compressedRecordData := r.RawBytes(int(batch.Length) - recordBatchOverhead)
		recordData := NewReader(r.Decompress(batch.Attributes.CompressionCodec(), compressedRecordData))
		for index := range batch.Records {
			batch.Records[index] = ReadRecord(messageVersion, recordData)
		}
		err := recordData.Error()
		if err != nil {
			r.err = err
		}
	}
	return batch
}

type RecordBatchAttributes int16

func (a RecordBatchAttributes) CompressionCodec() CompressionCodec {
	return CompressionCodec(int8(a) & compressionCodecMask)
}
func (a RecordBatchAttributes) TimestampType() TimestampType {
	return TimestampType(int8(a) & timestampTypeMask)
}
func (a RecordBatchAttributes) IsTransactional() bool {
	return int8(a)&isTransactionalMask == isTransactionalMask
}

func NewRecordBatchAttribute(codec CompressionCodec, isControl bool, timestampType TimestampType, isTransactional bool) RecordBatchAttributes {
	attributes := int16(codec) & int16(compressionCodecMask)
	if isControl {
		attributes |= controlMask
	}
	if timestampType == LogAppendTime {
		attributes |= timestampTypeMask
	}
	if isTransactional {
		attributes |= isTransactionalMask
	}
	return RecordBatchAttributes(attributes)
}

/*
Changelog
---------
- Headers added in KIP-82

Record =>
  Length => varint
  Attributes => int8
  TimestampDelta => varint
  OffsetDelta => varint
  KeyLen => varint
  Key => data
  ValueLen => varint
  Value => data
  Headers => [Header]


*/
type Record struct {
	Length         int64
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []Header
}

func (r Record) Write(messageVersion int8, w *Writer) {
	bw := NewWriter(nil)
	bw.Int8(r.Attributes)
	bw.Varlong(r.TimestampDelta)
	bw.Varlong(r.OffsetDelta)
	bw.VarintBytes(r.Key)
	bw.VarintBytes(r.Value)
	bw.Varlong(int64(len(r.Headers)))
	for _, header := range r.Headers {
		header.Write(messageVersion, bw)
	}
	recordData, err := bw.Data()
	if err != nil {
		w.err = err
		return
	}
	w.Varlong(int64(len(recordData)))
	w.RawBytes(recordData)
}

func ReadRecord(messageVersion int8, r *Reader) Record {
	record := Record{}
	record.Length = r.Varlong()
	record.Attributes = r.Int8()
	record.TimestampDelta = r.Varlong()
	record.OffsetDelta = r.Varlong()
	record.Key = r.VarintBytes()
	record.Value = r.VarintBytes()
	numHeaders := r.Varlong()
	if numHeaders >= 0 {
		record.Headers = make([]Header, numHeaders)
		for index := range record.Headers {
			record.Headers[index] = ReadHeader(messageVersion, r)
		}
	}
	return record
}

/*
Header => HeaderKey HeaderVal
  HeaderKeyLen => varint
  HeaderKey => string
  HeaderValueLen => varint
  HeaderValue => data
*/

type Header struct {
	Key   []byte
	Value []byte
}

func (r Header) Write(messageVersion int8, w *Writer) {
	w.VarintBytes(r.Key)
	w.VarintBytes(r.Value)
}

func ReadHeader(messageVersion int8, r *Reader) Header {
	header := Header{}
	header.Key = r.VarintBytes()
	header.Value = r.VarintBytes()
	return header
}
