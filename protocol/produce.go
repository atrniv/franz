package protocol

/*
Changelog
---------

- Version 1 and 2 are the same as version 0.
- Version 3 adds the transactional ID, which is used for authorization when attempting to write
 transactional data.  Version 3 also adds support for Kafka Message Format v2.
- Version 4 is the same as version 3, but the requestor must be prepared to handle a
 KAFKA_STORAGE_ERROR.
- Version 5 and 6 are the same as version 3.
- Starting in version 7, records can be produced using ZStandard compression.  See KIP-110.
- Starting in Version 8, response has RecordErrors and ErrorMEssage. See KIP-467.

v0, v1 (supported in 0.9.0 or later) and v2 (supported in 0.10.0 or later)
ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
  RequiredAcks => int16
  Timeout => int32
  Partition => int32
  MessageSetSize => int32
*/

type ProduceRequest struct {
	TransactionalId NullableString
	Acks            int16
	TimeoutMs       int32
	Topics          []TopicProduceData
}

type TopicProduceData struct {
	Name       string
	Partitions []PartitionProduceData
}

type PartitionProduceData struct {
	PartitionIndex int32
	RecordSize     int32
	Records        RecordData
}

func ReadProduceRequest(apiVersion int16, r *Reader) (ProduceRequest, error) {
	req := ProduceRequest{}
	if apiVersion >= 3 {
		req.TransactionalId = r.NullableString()
	}
	req.Acks = r.Int16()
	req.TimeoutMs = r.Int32()
	req.Topics = make([]TopicProduceData, r.Int32())
	for index := range req.Topics {
		req.Topics[index] = readTopicProduceData(apiVersion, r)
	}
	if err := r.Error(); err != nil {
		return req, err
	}

	return req, nil
}

func readTopicProduceData(apiVersion int16, r *Reader) TopicProduceData {
	data := TopicProduceData{}
	data.Name = r.String()
	data.Partitions = make([]PartitionProduceData, r.Int32())
	for index := range data.Partitions {
		data.Partitions[index] = readPartitionProduceData(apiVersion, r)
	}
	return data
}

func readPartitionProduceData(apiVersion int16, r *Reader) PartitionProduceData {
	data := PartitionProduceData{}
	data.PartitionIndex = r.Int32()
	data.RecordSize = r.Int32()
	if apiVersion >= 3 {
		data.Records = ReadRecordData(2, r)
	} else {
		data.Records = ReadRecordData(1, r)
	}
	return data
}

type ProduceResponse struct {
	Responses      []TopicProduceResponse
	ThrottleTimeMs int32
}

func (r ProduceResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	w.Int32(int32(len(r.Responses)))
	for _, response := range r.Responses {
		response.Write(apiVersion, w)
	}

	if apiVersion >= 1 {
		w.Int32(r.ThrottleTimeMs)
	}

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}

type TopicProduceResponse struct {
	Name       string
	Partitions []PartitionProduceResponse
}

func (r TopicProduceResponse) Write(apiVersion int16, w *Writer) {
	w.String(r.Name)
	w.Int32(int32(len(r.Partitions)))
	for _, partition := range r.Partitions {
		partition.Write(apiVersion, w)
	}
}

type PartitionProduceResponse struct {
	PartitionIndex  int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
	RecordErrors    []BatchIndexAndErrorMessage
	ErrorMessage    NullableString
}

func (r PartitionProduceResponse) Write(apiVersion int16, w *Writer) {
	w.Int32(r.PartitionIndex)
	w.Int16(r.ErrorCode)
	w.Int64(r.BaseOffset)

	if apiVersion >= 2 {
		w.Int64(r.LogAppendTimeMs)
	}
	if apiVersion >= 5 {
		w.Int64(r.LogStartOffset)
	}
	if apiVersion >= 8 {
		w.Int32(int32(len(r.RecordErrors)))
		for _, err := range r.RecordErrors {
			err.Write(apiVersion, w)
		}
		w.NullableString(r.ErrorMessage)
	}
}

type BatchIndexAndErrorMessage struct {
	BatchIndex             int32
	BatchIndexErrorMessage string
}

func (r BatchIndexAndErrorMessage) Write(apiVersion int16, w *Writer) {
	w.Int32(r.BatchIndex)
	w.String(r.BatchIndexErrorMessage)
}
