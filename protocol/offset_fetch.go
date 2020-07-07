package protocol

/*

Changelog
---------

- In version 0, the request read offsets from ZK.
- Starting in version 1, the broker supports fetching offsets from the internal __consumer_offsets topic.
- Starting in version 2, the request can contain a null topics array to indicate that offsets
  for all topics should be fetched. It also returns a top level error code for group or coordinator level errors.
- Version 3, 4, and 5 are the same as version 2.
- Version 6 is the first flexible version.
- Version 7 is adding the require stable flag.

OffsetFetch Request (Version: 0) => group_id [topics]
  group_id => STRING
  topics => topic [partitions]
    topic => STRING
    partitions => partition
	  partition => INT32

OffsetFetch Request (Version: 1) => group_id [topics]
  group_id => STRING
  topics => topic [partitions]
    topic => STRING
    partitions => partition
      partition => INT32
*/
type OffsetFetchRequest struct {
	GroupID       string
	Topics        []OffsetFetchTopic
	RequireStable bool
}

type OffsetFetchTopic struct {
	Name             string
	PartitionIndexes []int32
}

func ReadOffsetFetchRequest(r *Reader, apiVersion int16) (OffsetFetchRequest, error) {
	req := OffsetFetchRequest{}

	req.GroupID = r.String()
	req.Topics = make([]OffsetFetchTopic, int(r.Int32()))
	for index := range req.Topics {
		topic, err := readOffsetFetchTopic(r, apiVersion)
		if err != nil {
			return OffsetFetchRequest{}, err
		}
		req.Topics[index] = topic
	}
	if apiVersion >= 7 {
		req.RequireStable = r.Boolean()
	}

	if err := r.Error(); err != nil {
		return OffsetFetchRequest{}, err
	}

	return req, nil
}

func readOffsetFetchTopic(r *Reader, apiVersion int16) (OffsetFetchTopic, error) {
	req := OffsetFetchTopic{}

	req.Name = r.String()
	req.PartitionIndexes = make([]int32, int(r.Int32()))
	for index := range req.PartitionIndexes {
		req.PartitionIndexes[index] = r.Int32()
	}

	return req, nil
}

/*
Changelog
---------
- Version 1 is the same as version 0.
- Version 2 adds a top-level error code.
- Version 3 adds the throttle time.
- Starting in version 4, on quota violation, brokers send out responses before throttling.
- Version 5 adds the leader epoch to the committed offset.
- Version 6 is the first flexible version.
- Version 7 adds pending offset commit as new error response on partition level.

OffsetFetch Response (Version: 0) => [responses]
  responses => topic [partition_responses]
    topic => STRING
    partition_responses => partition offset metadata error_code
      partition => INT32
      offset => INT64
      metadata => NULLABLE_STRING
	  error_code => INT16

OffsetFetch Response (Version: 1) => [responses]
  responses => topic [partition_responses]
    topic => STRING
    partition_responses => partition offset metadata error_code
      partition => INT32
      offset => INT64
      metadata => NULLABLE_STRING
	  error_code => INT16
*/
type OffsetFetchResponse struct {
	ThrottleTimeMs int32
	Topics         []OffsetFetchTopicResponse
	ErrorCode      int16
}

func (r OffsetFetchResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {

	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 3 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int32(int32(len(r.Topics)))
	for _, topic := range r.Topics {
		topic.Write(apiVersion, w)
	}

	if apiVersion >= 2 {
		w.Int16(r.ErrorCode)
	}

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}

type OffsetFetchTopicResponse struct {
	Name       string
	Partitions []OffsetFetchTopicPartitionResponse
}

func (r OffsetFetchTopicResponse) Write(apiVersion int16, w *Writer) {
	w.String(r.Name)

	w.Int32(int32(len(r.Partitions)))
	for _, partition := range r.Partitions {
		partition.Write(apiVersion, w)
	}
}

type OffsetFetchTopicPartitionResponse struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	Metadata             NullableString
	ErrorCode            int16
}

func (r OffsetFetchTopicPartitionResponse) Write(apiVersion int16, w *Writer) {
	w.Int32(r.PartitionIndex)
	w.Int64(r.CommittedOffset)

	if apiVersion >= 5 {
		w.Int32(r.CommittedLeaderEpoch)
	}

	w.NullableString(r.Metadata)
	w.Int16(r.ErrorCode)
}
