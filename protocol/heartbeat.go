package protocol

type HearbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func ReadHearbeatRequest(r *Reader, apiVersion int16) (HearbeatRequest, error) {
	req := HearbeatRequest{}

	req.GroupID = r.String()
	req.GenerationID = r.Int32()
	req.MemberID = r.String()

	if err := r.Error(); err != nil {
		return HearbeatRequest{}, err
	}
	return req, nil
}

type HearbeatResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
}

func (r HearbeatResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	if apiVersion >= 1 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int16(r.ErrorCode)

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}
