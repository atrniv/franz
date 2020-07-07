package protocol

/*
Changelog
---------
- Version 1 removes MaxNumOffsets.  From this version forward, only a single
  offset can be returned.
- Version 2 adds the isolation level, which is used for transactional reads.
- Version 3 is the same as version 2.
- Version 4 adds the current leader epoch, which is used for fencing.
- Version 5 is the same as version 5.

// v0
ListOffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
  ReplicaId => int32
  TopicName => string
  Partition => int32
  Time => int64
  MaxNumberOfOffsets => int32


// v1 (supported in 0.10.1.0 and later)
ListOffsetRequest => ReplicaId [TopicName [Partition Time]]
  ReplicaId => int32
  TopicName => string
  Partition => int32
  Time => int64

*/

type ListOffsetRequest struct {
	ReplicaID      int32
	IsolationLevel int8
	Topics         []ListOffsetTopic
}

type ListOffsetTopic struct {
	Name       string
	Partitions []ListOffsetPartition
}

type ListOffsetPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32
	Timestamp          int64
	MaxNumberOfOffsets int32
}

func ReadListOffsetRequest(r *Reader, apiVersion int16) (ListOffsetRequest, error) {
	req := ListOffsetRequest{}

	req.ReplicaID = r.Int32()

	if apiVersion >= 2 {
		req.IsolationLevel = r.Int8()
	}

	req.Topics = make([]ListOffsetTopic, r.Int32())
	for index := range req.Topics {
		topic := readListOffsetTopic(r, apiVersion)
		req.Topics[index] = topic
	}

	if err := r.Error(); err != nil {
		return ListOffsetRequest{}, err
	}
	return req, nil
}

func readListOffsetTopic(r *Reader, apiVersion int16) ListOffsetTopic {
	topic := ListOffsetTopic{}
	topic.Name = r.String()
	topic.Partitions = make([]ListOffsetPartition, r.Int32())
	for index := range topic.Partitions {
		partition := readListOffsetPartition(r, apiVersion)
		topic.Partitions[index] = partition
	}
	return topic
}

func readListOffsetPartition(r *Reader, apiVersion int16) ListOffsetPartition {
	partition := ListOffsetPartition{}

	partition.PartitionIndex = r.Int32()

	if apiVersion >= 4 {
		partition.CurrentLeaderEpoch = r.Int32()
	}

	partition.Timestamp = r.Int64()

	if apiVersion < 1 {
		partition.MaxNumberOfOffsets = r.Int32()
	}

	return partition
}

/*
Changelog
--------
- Version 1 removes the offsets array in favor of returning a single offset.
  Version 1 also adds the timestamp associated with the returned offset.
- Version 2 adds the throttle time.
- Starting in version 3, on quota violation, brokers send out responses before throttling.
- Version 4 adds the leader epoch, which is used for fencing.
- Version 5 adds a new error code, OFFSET_NOT_AVAILABLE.

// v0
ListOffsetResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode [Offset]
  Partition => int32
  ErrorCode => int16
  Offset => int64


// v1
ListOffsetResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode Timestamp [Offset]
  Partition => int32
  ErrorCode => int16
  Timestamp => int64
  Offset => int64
*/

type ListOffsetResponse struct {
	ThrottleTimeMs int32
	Topics         []ListOffsetTopicResponse
	ErrorCode      int16
}

func (r ListOffsetResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 2 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int32(int32(len(r.Topics)))
	for _, topic := range r.Topics {
		topic.Write(apiVersion, w)
	}

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}

func ReadListOffsetResponse(apiVersion int16, r *Reader) (ListOffsetResponse, ResponseHeader, error) {
	header := ReadResponseHeader(apiVersion, r)
	res := ListOffsetResponse{}

	if apiVersion >= 2 {
		res.ThrottleTimeMs = r.Int32()
	}

	res.Topics = make([]ListOffsetTopicResponse, r.Int32())
	for index := range res.Topics {
		res.Topics[index] = ReadListOffsetTopicResponse(apiVersion, r)
	}

	return res, header, r.Error()
}

type ListOffsetTopicResponse struct {
	Name       string
	Partitions []ListOffsetPartitionResponse
}

func (r ListOffsetTopicResponse) Write(apiVersion int16, w *Writer) error {
	w.String(r.Name)

	w.Int32(int32(len(r.Partitions)))
	for _, partition := range r.Partitions {
		partition.Write(apiVersion, w)
	}

	return nil
}

func ReadListOffsetTopicResponse(apiVersion int16, r *Reader) ListOffsetTopicResponse {
	res := ListOffsetTopicResponse{}
	res.Name = r.String()
	res.Partitions = make([]ListOffsetPartitionResponse, r.Int32())
	for index := range res.Partitions {
		res.Partitions[index] = ReadListOffsetPartitionResponse(apiVersion, r)
	}
	return res
}

type ListOffsetPartitionResponse struct {
	PartitionIndex  int32
	ErrorCode       int16
	OldStyleOffsets []int64
	Timestamp       int64
	Offset          int64
	LeaderEpoch     int32
}

func (r ListOffsetPartitionResponse) Write(apiVersion int16, w *Writer) {
	w.Int32(r.PartitionIndex)

	w.Int16(r.ErrorCode)

	if apiVersion >= 1 {
		w.Int64(r.Timestamp)
		w.Int64(r.Offset)
	} else {
		w.Int32(int32(len(r.OldStyleOffsets)))
		for _, offset := range r.OldStyleOffsets {
			w.Int64(offset)
		}
	}

	if apiVersion >= 4 {
		w.Int32(r.LeaderEpoch)
	}
}

func ReadListOffsetPartitionResponse(apiVersion int16, r *Reader) ListOffsetPartitionResponse {
	res := ListOffsetPartitionResponse{}
	res.PartitionIndex = r.Int32()
	res.ErrorCode = r.Int16()

	if apiVersion >= 1 {
		res.Timestamp = r.Int64()
		res.Offset = r.Int64()
	} else {
		res.OldStyleOffsets = make([]int64, r.Int32())
		for index := range res.OldStyleOffsets {
			res.OldStyleOffsets[index] = r.Int64()
		}
	}
	if apiVersion >= 4 {
		res.LeaderEpoch = r.Int32()
	}
	return res
}
