package protocol

type MetadataRequest struct {
	Topics                             []string
	AllowTopicAutoCreation             bool
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
	AllTopicsMetadata                  bool
}

func ReadMetadataRequest(r *Reader, apiVersion int16) (MetadataRequest, error) {
	req := MetadataRequest{
		Topics: []string{},
	}

	as := r.Int32()
	for as > 0 {
		topic := r.String()
		req.Topics = append(req.Topics, topic)
		as--
	}
	if apiVersion == 0 {
		if as == 0 {
			req.AllTopicsMetadata = true
		}
	} else {
		if as == -1 {
			req.AllTopicsMetadata = true
		}
	}
	if apiVersion >= 4 {
		req.AllowTopicAutoCreation = r.Boolean()
	}

	if apiVersion >= 8 {
		req.IncludeClusterAuthorizedOperations = r.Boolean()
		req.IncludeTopicAuthorizedOperations = r.Boolean()
	}
	if err := r.Error(); err != nil {
		return MetadataRequest{}, err
	}
	return req, nil
}

type MetadataResponse struct {
	Brokers                     []BrokerMetadata
	Topics                      []TopicMetadata
	ControllerID                int32
	ClusterID                   NullableString
	ThrottleTimeMs              int32
	ClusterAuthorizedOperations int32
}

func (r MetadataResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 3 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int32(int32(len(r.Brokers)))
	for _, broker := range r.Brokers {
		broker.Write(apiVersion, w)
	}

	if apiVersion >= 2 {
		w.NullableString(r.ClusterID)
	}

	if apiVersion >= 1 {
		w.Int32(r.ControllerID)
	}

	w.Int32(int32(len(r.Topics)))
	for _, topic := range r.Topics {
		topic.Write(apiVersion, w)
	}

	if apiVersion >= 8 {
		w.Int32(r.ClusterAuthorizedOperations)
	}

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}

type BrokerMetadata struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   NullableString
}

func (r BrokerMetadata) Write(apiVersion int16, w *Writer) {
	w.Int32(r.NodeID)
	w.String(r.Host)
	w.Int32(r.Port)
	if apiVersion >= 1 {
		w.NullableString(r.Rack)
	}
}

type TopicMetadata struct {
	ErrorCode                 int16
	Name                      string
	IsInternal                bool
	Partitions                []PartitionMetadata
	TopicAuthorizedOperations int32
}

func (r TopicMetadata) Write(apiVersion int16, w *Writer) {
	w.Int16(r.ErrorCode)
	w.String(r.Name)

	if apiVersion >= 1 {
		w.Boolean(r.IsInternal)
	}

	w.Int32(int32(len(r.Partitions)))
	for _, partition := range r.Partitions {
		partition.Write(apiVersion, w)
	}

	if apiVersion >= 8 {
		w.Int32(r.TopicAuthorizedOperations)
	}
}

type PartitionMetadata struct {
	ErrorCode           int16
	PartitionIndex      int32
	LeaderID            int32
	LeaderEpoch         int32
	ReplicaNodes        []int32
	ISRNodes            []int32
	OfflineReplicaNodes []int32
}

func (r PartitionMetadata) Write(apiVersion int16, w *Writer) {
	w.Int16(r.ErrorCode)
	w.Int32(r.PartitionIndex)
	w.Int32(r.LeaderID)

	if apiVersion >= 7 {
		w.Int32(r.LeaderID)
	}

	w.Int32(int32(len(r.ReplicaNodes)))
	for _, id := range r.ReplicaNodes {
		w.Int32(id)
	}
	w.Int32(int32(len(r.ISRNodes)))
	for _, id := range r.ISRNodes {
		w.Int32(id)
	}
	if apiVersion >= 5 {
		w.Int32(int32(len(r.OfflineReplicaNodes)))
		for _, id := range r.OfflineReplicaNodes {
			w.Int32(id)
		}
	}
}
