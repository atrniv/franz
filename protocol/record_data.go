package protocol

type RecordData struct {
	Version     int8
	Size        int32
	MessageSet  MessageSet
	RecordBatch RecordBatch
}

func ReadRecordData(messageVersion int8, r *Reader) RecordData {
	data := RecordData{}
	data.Version = messageVersion
	if messageVersion < 2 {
		data.MessageSet = ReadMessageSet(messageVersion, r)
	} else {
		data.RecordBatch = ReadRecordBatch(messageVersion, r)
	}
	return data
}

func (r RecordData) Write(w *Writer) {
	if r.Version < 2 {
		r.MessageSet.Write(r.Version, w)
	} else {
		r.RecordBatch.Write(r.Version, w)
	}
}
