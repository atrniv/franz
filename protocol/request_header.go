package protocol

type RequestHeader struct {
	CorrelationID int32
	ClientID      NullableString
	TaggedFields  []TagField
}

func ReadRequestHeader(r *Reader, apiVersion int16) (RequestHeader, error) {
	header := RequestHeader{}
	header.CorrelationID = r.Int32()
	header.ClientID = r.NullableString()
	// if apiVersion >= 9 {
	// 	header.TaggedFields = r.TagBuffer()
	// }
	return header, nil
}
