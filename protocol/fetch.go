package protocol

/*
Changelog
---------
- Version 1 is the same as version 0.
- Starting in Version 2, the requestor must be able to handle Kafka Log
  Message format version 1.
- Version 3 adds MaxBytes.  Starting in version 3, the partition ordering in
  the request is now relevant.  Partitions will be processed in the order
  they appear in the request.
- Version 4 adds IsolationLevel.  Starting in version 4, the reqestor must be
  able to handle Kafka log message format version 2.
- Version 5 adds LogStartOffset to indicate the earliest available offset of
  partition data that can be consumed.
- Version 6 is the same as version 5.
- Version 7 adds incremental fetch request support, as described in KIP-227
- Version 8 is the same as version 7.
- Version 9 adds CurrentLeaderEpoch, as described in KIP-320.
- Version 10 indicates that we can use the ZStd compression algorithm, as
  described in KIP-110.



*/

type FetchRequest struct {
	ReplicaID      int32
	MaxWait        int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	SessionID      int32
	Epoch          int32
	Topics         []FetchableTopic
	Forgotten      []ForgottenTopic
	RackID         string
}

func ReadFetchRequest(r *Reader, apiVersion int16) (FetchRequest, error) {
	req := FetchRequest{}

	req.ReplicaID = r.Int32()
	req.MaxWait = r.Int32()
	req.MinBytes = r.Int32()

	if apiVersion >= 3 {
		req.MaxBytes = r.Int32()
	}
	if apiVersion >= 4 {
		req.IsolationLevel = r.Int8()
	}
	if apiVersion >= 7 {
		req.SessionID = r.Int32()
		req.Epoch = r.Int32()
	}

	req.Topics = make([]FetchableTopic, r.Int32())
	for index := range req.Topics {
		topic := readFetchableTopic(r, apiVersion)
		req.Topics[index] = topic
	}

	if apiVersion >= 7 {
		req.Forgotten = make([]ForgottenTopic, r.Int32())
		for index := range req.Forgotten {
			topic := readForgottenTopic(r, apiVersion)
			req.Forgotten[index] = topic
		}
	}

	if apiVersion >= 11 {
		req.RackID = r.String()
	}

	if err := r.Error(); err != nil {
		return req, err
	}
	return req, nil
}

type FetchableTopic struct {
	Name            string
	FetchPartitions []FetchPartition
}

func readFetchableTopic(r *Reader, apiVersion int16) FetchableTopic {
	topic := FetchableTopic{}
	topic.Name = r.String()
	topic.FetchPartitions = make([]FetchPartition, r.Int32())
	for index := range topic.FetchPartitions {
		partition := readFetchPartition(r, apiVersion)
		topic.FetchPartitions[index] = partition
	}
	return topic
}

type FetchPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LogStartOffset     int64
	MaxBytes           int32
}

func readFetchPartition(r *Reader, apiVersion int16) FetchPartition {
	partition := FetchPartition{}
	partition.PartitionIndex = r.Int32()
	if apiVersion >= 9 {
		partition.CurrentLeaderEpoch = r.Int32()
	}
	partition.FetchOffset = r.Int64()
	if apiVersion >= 5 {
		partition.LogStartOffset = r.Int64()
	}
	partition.MaxBytes = r.Int32()
	return partition
}

type ForgottenTopic struct {
	Name                      string
	ForgottenPartitionIndexes []int32
}

func readForgottenTopic(r *Reader, apiVersion int16) ForgottenTopic {
	topic := ForgottenTopic{}
	topic.Name = r.String()
	topic.ForgottenPartitionIndexes = make([]int32, r.Int32())
	for index := range topic.ForgottenPartitionIndexes {
		topic.ForgottenPartitionIndexes[index] = r.Int32()
	}
	return topic
}

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Topics         []FetchableTopicResponse
}

func (r FetchResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 1 {
		w.Int32(r.ThrottleTimeMs)
	}

	if apiVersion >= 7 {
		w.Int16(r.ErrorCode)
		w.Int32(r.SessionID)
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

func ReadFetchResponse(apiVersion int16, r *Reader) (FetchResponse, ResponseHeader, error) {
	header := ReadResponseHeader(apiVersion, r)
	res := FetchResponse{}

	if apiVersion >= 1 {
		res.ThrottleTimeMs = r.Int32()
	}

	if apiVersion >= 7 {
		res.ErrorCode = r.Int16()
		res.SessionID = r.Int32()
	}

	res.Topics = make([]FetchableTopicResponse, r.Int32())
	for index := range res.Topics {
		res.Topics[index] = ReadFetchableTopicResponse(apiVersion, r)
	}

	return res, header, r.Error()
}

type FetchableTopicResponse struct {
	Name       string
	Partitions []FetchablePartitionResponse
}

func (r FetchableTopicResponse) Write(apiVersion int16, w *Writer) {
	w.String(r.Name)
	w.Int32(int32(len(r.Partitions)))
	for _, partition := range r.Partitions {
		partition.Write(apiVersion, w)
	}
}

func ReadFetchableTopicResponse(apiVersion int16, r *Reader) FetchableTopicResponse {
	res := FetchableTopicResponse{}
	res.Name = r.String()
	res.Partitions = make([]FetchablePartitionResponse, r.Int32())
	for index := range res.Partitions {
		res.Partitions[index] = ReadFetchablePartitionResponse(apiVersion, r)
	}
	return res
}

type FetchablePartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	Aborted              []AbortedTransaction
	PreferredReadReplica int32
	Records              []byte
}

func (r FetchablePartitionResponse) Write(apiVersion int16, w *Writer) {
	w.Int32(r.PartitionIndex)
	w.Int16(r.ErrorCode)
	w.Int64(r.HighWatermark)

	if apiVersion >= 4 {
		w.Int64(r.LastStableOffset)
	}
	if apiVersion >= 5 {
		w.Int64(r.LogStartOffset)
	}

	if apiVersion >= 4 {
		w.Int32(int32(len(r.Aborted)))
		for _, aborted := range r.Aborted {
			aborted.Write(apiVersion, w)
		}
	}

	if apiVersion >= 11 {
		w.Int32(r.PreferredReadReplica)
	}

	w.Bytes(r.Records)
}

func ReadFetchablePartitionResponse(apiVersion int16, r *Reader) FetchablePartitionResponse {
	res := FetchablePartitionResponse{}
	res.PartitionIndex = r.Int32()
	res.ErrorCode = r.Int16()
	res.HighWatermark = r.Int64()

	if apiVersion >= 4 {
		res.LastStableOffset = r.Int64()
	}
	if apiVersion >= 5 {
		res.LogStartOffset = r.Int64()
	}

	if apiVersion >= 4 {
		abortedLen := r.Int32()
		if abortedLen > 0 {
			res.Aborted = make([]AbortedTransaction, abortedLen)
			for index := range res.Aborted {
				res.Aborted[index] = ReadAbortedTransaction(apiVersion, r)
			}
		}
	}

	if apiVersion >= 11 {
		res.PreferredReadReplica = r.Int32()
	}

	res.Records = r.Bytes()

	return res
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (r AbortedTransaction) Write(apiVersion int16, w *Writer) {
	w.Int64(r.ProducerID)
	w.Int64(r.FirstOffset)
}

func ReadAbortedTransaction(apiVersion int16, r *Reader) AbortedTransaction {
	res := AbortedTransaction{}
	res.ProducerID = r.Int64()
	res.FirstOffset = r.Int64()
	return AbortedTransaction{}
}
