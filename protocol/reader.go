package protocol

import (
	"encoding/binary"
	"math"
	"unicode/utf8"
)

type Reader struct {
	pos  int
	err  error
	data []byte
}

func NewReader(data []byte) *Reader {
	return &Reader{
		pos:  0,
		data: data,
		err:  nil,
	}
}

func (r *Reader) Error() error {
	if r.err == nil {
		if r.pos < len(r.data) {
			return NewProtocolException("message_not_finished", "Message has %d bytes left to be read", len(r.data)-r.pos)
		}
	}
	return r.err
}

func (r *Reader) Boolean() bool {
	if r.err != nil {
		return false
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return false
	}
	v8 := int8(r.data[r.pos])
	r.pos++
	return v8 > 0
}

func (r *Reader) Int8() int8 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	v8 := int8(r.data[r.pos])
	r.pos++
	return v8
}

func (r *Reader) Int16() int16 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	uv16 := binary.BigEndian.Uint16(r.data[r.pos : r.pos+2])
	r.pos += 2
	return int16(uv16)
}

func (r *Reader) Int32() int32 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	uv32 := binary.BigEndian.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return int32(uv32)
}

func (r *Reader) RawBytes(len int) []byte {
	data := r.data[r.pos : r.pos+len]
	r.pos += len
	return data
}

func (r *Reader) Int64() int64 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	uv64 := binary.BigEndian.Uint64(r.data[r.pos : r.pos+8])
	r.pos += 8
	return int64(uv64)
}

func (r *Reader) Uint32() uint32 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	uv32 := binary.BigEndian.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return uv32
}

func (r *Reader) Uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	uv64, n := binary.Uvarint(r.data[r.pos:])
	if n <= 0 {
		r.err = NewProtocolException("invalid_varint", "Invalid varint data")
		return 0
	}
	r.pos += n
	return uv64
}

func (r *Reader) Varint() int32 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	v64, n := binary.Varint(r.data[r.pos:])
	if n <= 0 {
		r.err = NewProtocolException("invalid_varint", "Invalid varint data")
		return 0
	}
	r.pos += n
	return int32(v64)
}

func (r *Reader) Varlong() int64 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	v64, n := binary.Varint(r.data[r.pos:])
	if n <= 0 {
		r.err = NewProtocolException("invalid_varint", "Invalid varint data")
		return 0
	}
	r.pos += n
	return v64
}

func (r *Reader) UUID() UUID {
	if r.err != nil {
		return UUID{}
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return UUID{}
	}
	vuid := UUID{}
	copy(vuid[:], r.data[r.pos:r.pos+16])
	r.pos += 16
	return vuid
}

func (r *Reader) Float64() float64 {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = NewProtocolException("message_eof", "No more data available to be read")
		return 0
	}
	v64 := binary.BigEndian.Uint64(r.data[r.pos:+8])
	r.pos += 8
	return math.Float64frombits(v64)
}

func (r *Reader) String() string {
	l := r.Int16()
	if r.err != nil {
		return ""
	}

	if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return ""
	}

	sr := []rune{}
	for l > 0 {
		if r.pos >= len(r.data) {
			r.err = NewProtocolException("message_eof", "No more data available to be read")
			return ""
		}
		rn, n := utf8.DecodeRune(r.data[r.pos:])
		sr = append(sr, rn)
		l -= int16(n)
		r.pos += n
	}
	return string(sr)
}

func (r *Reader) CompactString() string {
	l := r.Varlong()
	if r.err != nil {
		return ""
	}

	if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return ""
	}

	sr := []rune{}
	for l > 0 {
		if r.pos >= len(r.data) {
			r.err = NewProtocolException("message_eof", "No more data available to be read")
			return ""
		}
		rn, n := utf8.DecodeRune(r.data[r.pos:])
		sr = append(sr, rn)
		l -= int64(n)
		r.pos += n
	}
	return string(sr)
}

func (r *Reader) NullableString() NullableString {
	l := r.Int16()
	if r.err != nil {
		return EmptyNullableString
	}

	if l == -1 {
		return NullableString{IsValid: true, IsNull: true}
	} else if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return EmptyNullableString
	}

	sr := []rune{}
	for l > 0 {
		if r.pos >= len(r.data) {
			r.err = NewProtocolException("message_eof", "No more data available to be read")
			return EmptyNullableString
		}
		rn, n := utf8.DecodeRune(r.data[r.pos:])
		sr = append(sr, rn)
		l -= int16(n)
		r.pos += n
	}
	return NullableString{IsValid: true, IsNull: false, Value: string(sr)}
}

func (r *Reader) CompactNullableString() NullableString {
	l := r.Varlong()
	if r.err != nil {
		return EmptyNullableString
	}

	if l == 0 {
		return NullableString{IsValid: true, IsNull: true}
	} else if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return EmptyNullableString
	}

	sr := []rune{}
	for l > 0 {
		if r.pos >= len(r.data) {
			r.err = NewProtocolException("message_eof", "No more data available to be read")
			return EmptyNullableString
		}
		rn, n := utf8.DecodeRune(r.data[r.pos:])
		sr = append(sr, rn)
		l -= int64(n)
		r.pos += n
	}
	return NullableString{IsValid: true, IsNull: false, Value: string(sr)}
}

func (r *Reader) Bytes() []byte {
	l := r.Int32()
	if r.err != nil {
		return nil
	}
	if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return nil
	}
	data := r.data[r.pos : r.pos+int(l)]
	r.pos += int(l)
	return data
}

func (r *Reader) CompactBytes() []byte {
	l := r.Uvarint()
	if r.err != nil {
		return nil
	}
	if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return nil
	}
	data := r.data[r.pos : r.pos+int(l)-1]
	r.pos += int(l) - 1
	return data
}

func (r *Reader) NullableBytes() []byte {
	l := r.Int32()
	if r.err != nil {
		return nil
	}
	if l == -1 {
		return nil
	} else if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return nil
	}
	data := r.data[r.pos : r.pos+int(l)]
	r.pos += int(l)
	return data
}

func (r *Reader) CompactNullableBytes() []byte {
	l := r.Uvarint()
	if r.err != nil {
		return nil
	}
	if l == 0 {
		return nil
	} else if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return nil
	}
	data := r.data[r.pos : r.pos+int(l)-1]
	r.pos += int(l) - 1
	return data
}

func (r *Reader) VarintBytes() []byte {
	l := r.Varlong()
	if r.err != nil {
		return nil
	}
	if l == 0 {
		return nil
	} else if l < 0 {
		r.err = NewProtocolException("invalid_size", "Invalid size provided for this field")
		return nil
	}
	data := r.data[r.pos : r.pos+int(l)]
	r.pos += int(l)
	return data
}

func (r *Reader) TagBuffer() []TagField {
	l := r.Uvarint()
	fields := []TagField{}
	for l > 0 {
		t := r.Uvarint()
		tl := r.Uvarint()
		data := make([]byte, int(tl))
		copy(data, r.data[r.pos:r.pos+int(tl)])
		fields = append(fields, TagField{
			Type:   t,
			Length: tl,
			Data:   data,
		})
		l--
	}
	return fields
}

func (r *Reader) APIMeta() (APIKeyEnum, int16) {
	key := APIKeyEnum(r.Int16())
	version := r.Int16()
	return key, version
}
