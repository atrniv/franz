package protocol

type APIVersionsRequest struct {
	ClientSoftwareName    string
	ClientSoftwareVersion string
}

func ReadAPIVersionRequest(r *Reader, apiVersion int16) (APIVersionsRequest, error) {
	req := APIVersionsRequest{}

	if apiVersion >= 3 {
		req.ClientSoftwareName = r.String()
		req.ClientSoftwareName = r.String()
	}

	if err := r.Error(); err != nil {
		return APIVersionsRequest{}, err
	}
	return req, nil
}

type APIVersionsResponse struct {
	ErrorCode      int16
	APIKeys        []APIVersionsResponseKey
	ThrottleTimeMs int32
}

func (r APIVersionsResponse) Write(apiVersion int16, header RequestHeader, w *Writer) error {
	res := ResponseHeader{CorrelationID: header.CorrelationID}
	res.Write(apiVersion, w)

	w.Int16(r.ErrorCode)

	w.Int32(int32(len(r.APIKeys)))
	for _, key := range r.APIKeys {
		key.Write(apiVersion, w)
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

type APIVersionsResponseKey struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func (r APIVersionsResponseKey) Write(apiVersion int16, w *Writer) {
	w.Int16(r.APIKey)
	w.Int16(r.MinVersion)
	w.Int16(r.MaxVersion)
}
