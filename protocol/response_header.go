package protocol

type ResponseHeader struct {
	CorrelationID int32
	TaggedFields  []TagField
}

func (r ResponseHeader) Write(apiVersion int16, w *Writer) {
	w.Int32(r.CorrelationID)
	if apiVersion >= 9 {
		// TODO: Write tagged fields
	}
}

func ReadResponseHeader(apiVersion int16, r *Reader) ResponseHeader {
	res := ResponseHeader{}
	res.CorrelationID = r.Int32()
	if apiVersion >= 9 {

	}
	return res
}
