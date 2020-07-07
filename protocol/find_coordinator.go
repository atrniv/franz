package protocol

type FindCoordinatorRequest struct {
	Key          string
	KeyType      KeyTypeEnum
	TaggedFields []TagField
}

func ReadFindCoordinatorRequest(r *Reader, apiVersion int16) (FindCoordinatorRequest, error) {
	req := FindCoordinatorRequest{}

	if apiVersion >= 3 {
		req.Key = r.CompactString()
	} else {
		req.Key = r.String()
	}

	if apiVersion >= 1 {
		req.KeyType = KeyTypeEnum(r.Int8())
	} else {
		req.KeyType = KeyTypeGroup
	}

	if apiVersion >= 3 {
		req.TaggedFields = r.TagBuffer()
	}

	if err := r.Error(); err != nil {
		return FindCoordinatorRequest{}, err
	}

	return req, nil
}

type FindCoordinatorResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   NullableString
	NodeID         int32
	Host           string
	Port           int32
}

func (r FindCoordinatorResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}

	res.Write(apiVersion, w)

	if apiVersion >= 1 {
		w.Int32(r.ThrottleTimeMs)
	}

	w.Int16(r.ErrorCode)

	if apiVersion >= 3 {
		w.CompactNullableString(r.ErrorMessage)
	} else if apiVersion >= 1 {
		w.NullableString(r.ErrorMessage)
	}

	w.Int32(r.NodeID)

	if apiVersion >= 3 {
		w.CompactString(r.Host)
	} else {
		w.String(r.Host)
	}

	w.Int32(r.Port)

	err := w.Close()
	if err != nil {
		return err
	}

	return nil
}
