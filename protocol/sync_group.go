package protocol

type SyncGroupRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupAssigments []SyncGroupAssignment
}

func (r SyncGroupRequest) IsFollower() bool {
	return len(r.GroupAssigments) == 0
}

func (r SyncGroupRequest) IsLeader() bool {
	return len(r.GroupAssigments) > 0
}

type SyncGroupAssignment struct {
	MemberID         string
	MemberAssignment []byte
}

func ReadSyncGroupRequest(r *Reader, apiVersion int16) (SyncGroupRequest, error) {
	req := SyncGroupRequest{}

	req.GroupID = r.String()
	req.GenerationID = r.Int32()
	req.MemberID = r.String()

	req.GroupAssigments = make([]SyncGroupAssignment, r.Int32())
	for index := range req.GroupAssigments {
		ga, err := readSyncGroupAssignment(r, apiVersion)
		if err != nil {
			return SyncGroupRequest{}, err
		}
		req.GroupAssigments[index] = ga
	}

	if err := r.Error(); err != nil {
		return SyncGroupRequest{}, err
	}
	return req, nil
}

func readSyncGroupAssignment(r *Reader, apiVersion int16) (SyncGroupAssignment, error) {
	assignment := SyncGroupAssignment{}
	assignment.MemberID = r.String()
	assignment.MemberAssignment = r.Bytes()
	return assignment, nil
}

type SyncGroupResponse struct {
	ThrottleTimeMs   int32
	ErrorCode        int16
	MemberAssignment []byte
}

func (r SyncGroupResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 1 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int16(r.ErrorCode)
	w.Bytes(r.MemberAssignment)

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}
