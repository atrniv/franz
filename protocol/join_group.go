package protocol

type JoinGroupRequest struct {
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           string
	GroupInstanceID    NullableString
	ProtocolType       string
	Protocols          []JoinGroupProtocol
	TaggedFields       []TagField
}

type JoinGroupProtocol struct {
	Name         string
	Metadata     []byte
	TaggedFields []TagField
}

func ReadJoinGroupRequest(r *Reader, apiVersion int16) (JoinGroupRequest, error) {
	req := JoinGroupRequest{}

	req.GroupID = r.String()
	req.SessionTimeoutMs = r.Int32()

	if apiVersion >= 1 {
		req.RebalanceTimeoutMs = r.Int32()
	}

	req.MemberID = r.String()

	if apiVersion >= 5 {
		req.GroupInstanceID = r.CompactNullableString()
	}

	req.ProtocolType = r.String()

	req.Protocols = make([]JoinGroupProtocol, r.Int32())
	for index := range req.Protocols {
		pc, err := readJoinGroupRequestProtocol(r, apiVersion)
		if err != nil {
			return JoinGroupRequest{}, err
		}
		req.Protocols[index] = pc
	}

	if apiVersion >= 6 {
		req.TaggedFields = r.TagBuffer()
	}

	if err := r.Error(); err != nil {
		return JoinGroupRequest{}, err
	}
	return req, nil
}

func readJoinGroupRequestProtocol(r *Reader, apiVersion int16) (JoinGroupProtocol, error) {
	req := JoinGroupProtocol{}
	req.Name = r.String()
	req.Metadata = r.Bytes()

	if apiVersion >= 6 {
		req.TaggedFields = r.TagBuffer()
	}

	return req, nil
}

type JoinGroupResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	GenerationID   int32
	ProtocolType   NullableString
	ProtocolName   NullableString
	Leader         string
	MemberID       string
	Members        []JoinGroupMember
}

func (r JoinGroupResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 2 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int16(r.ErrorCode)

	w.Int32(r.GenerationID)

	if apiVersion >= 7 {
		w.CompactNullableString(r.ProtocolType)
	}

	if apiVersion >= 7 {
		w.CompactNullableString(r.ProtocolName)
	} else if apiVersion >= 6 {
		w.CompactString(r.ProtocolName.Value)
	} else {
		w.String(r.ProtocolName.Value)
	}

	if apiVersion >= 6 {
		w.CompactString(r.Leader)
	} else {
		w.String(r.Leader)
	}

	if apiVersion >= 6 {
		w.CompactString(r.MemberID)
	} else {
		w.String(r.MemberID)
	}

	w.Int32(int32(len(r.Members)))
	for _, member := range r.Members {
		member.Write(apiVersion, w)
	}

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}

type JoinGroupMember struct {
	MemberID        string
	GroupInstanceID NullableString
	Metadata        []byte
	TaggedFields    []TagField
}

func (r JoinGroupMember) Write(apiVersion int16, w *Writer) {
	if apiVersion >= 6 {
		w.CompactString(r.MemberID)
	} else {
		w.String(r.MemberID)
	}

	if apiVersion >= 6 {
		w.CompactNullableString(r.GroupInstanceID)
	} else if apiVersion >= 5 {
		w.NullableString(r.GroupInstanceID)
	}

	if apiVersion >= 5 {
		w.CompactBytes(r.Metadata)
	} else {
		w.Bytes(r.Metadata)
	}
}
