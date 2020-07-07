package protocol

type OffsetCommitRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string
	RetentionTimeMs int64
	Topics          []OffsetCommitRequestTopic
}

type OffsetCommitRequestTopic struct {
	Name       string
	Partitions []OffsetCommitRequestPartition
}

type OffsetCommitRequestPartition struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	CommitTimestamp      int64
	CommittedMetadata    string
}

func ReadOffsetCommitRequest(r *Reader, apiVersion int16) (OffsetCommitRequest, error) {
	req := OffsetCommitRequest{}

	req.GroupID = r.String()

	if apiVersion >= 1 {
		req.GenerationID = r.Int32()
		req.MemberID = r.String()
	}
	if apiVersion >= 7 {
		req.GroupInstanceID = r.String()
	}
	if apiVersion >= 2 && apiVersion <= 4 {
		req.RetentionTimeMs = r.Int64()
	}

	req.Topics = make([]OffsetCommitRequestTopic, r.Int32())
	for index := range req.Topics {
		req.Topics[index] = readOffsetCommitRequestTopic(r, apiVersion)
	}

	return req, nil
}

func readOffsetCommitRequestTopic(r *Reader, apiVersion int16) OffsetCommitRequestTopic {
	req := OffsetCommitRequestTopic{}
	req.Name = r.String()
	req.Partitions = make([]OffsetCommitRequestPartition, r.Int32())
	for index := range req.Partitions {
		req.Partitions[index] = readOffsetCommitRequestPartition(r, apiVersion)
	}
	return req
}

func readOffsetCommitRequestPartition(r *Reader, apiVersion int16) OffsetCommitRequestPartition {
	req := OffsetCommitRequestPartition{}
	req.PartitionIndex = r.Int32()
	req.CommittedOffset = r.Int64()
	if apiVersion >= 6 {
		req.CommittedLeaderEpoch = r.Int32()
	}
	if apiVersion == 1 {
		req.CommitTimestamp = r.Int64()
	}
	req.CommittedMetadata = r.String()
	return req
}

type OffsetCommitResponse struct {
	ThrottleTimeMs int32
	Topics         []OffsetCommitResponseTopic
}

func (r OffsetCommitResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 3 {
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

type OffsetCommitResponseTopic struct {
	Name       string
	Partitions []OffsetCommitResponsePartition
}

func (r OffsetCommitResponseTopic) Write(apiVersion int16, w *Writer) {
	w.String(r.Name)
	w.Int32(int32(len(r.Partitions)))
	for _, partition := range r.Partitions {
		partition.Write(apiVersion, w)
	}
}

type OffsetCommitResponsePartition struct {
	PartitionIndex int32
	ErrorCode      int16
}

func (r OffsetCommitResponsePartition) Write(apiVersion int16, w *Writer) {
	w.Int32(r.PartitionIndex)
	w.Int16(r.ErrorCode)
}
