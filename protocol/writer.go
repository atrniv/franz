package protocol

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"unicode/utf8"
)

type Writer struct {
	output io.Writer
	err    error
	buffer *bytes.Buffer
}

func NewWriter(writer io.Writer) *Writer {
	return &Writer{
		output: writer,
		buffer: bytes.NewBuffer([]byte{}),
	}
}

func (w *Writer) Boolean(value bool) {
	if w.err != nil {
		return
	}
	if value {
		vb := int8(1)
		_, err := w.buffer.Write([]byte{byte(vb)})
		if err != nil {
			w.err = err
		}
	} else {
		vb := int8(0)
		_, err := w.buffer.Write([]byte{byte(vb)})
		if err != nil {
			w.err = err
		}
	}
}

func (w *Writer) Int8(value int8) {
	if w.err != nil {
		return
	}
	_, err := w.buffer.Write([]byte{byte(value)})
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Int16(value int16) {
	if w.err != nil {
		return
	}
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(value))
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Int32(value int32) {
	if w.err != nil {
		return
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(value))
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Int64(value int64) {
	if w.err != nil {
		return
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(value))
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Uint32(value uint32) {
	if w.err != nil {
		return
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, value)
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Uvarint(value uint64) {
	if w.err != nil {
		return
	}
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(b, value)
	_, err := w.buffer.Write(b[0:n])
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Varint(value int32) {
	if w.err != nil {
		return
	}
	b := make([]byte, binary.MaxVarintLen32)
	n := binary.PutVarint(b, int64(value))
	_, err := w.buffer.Write(b[0:n])
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Varlong(value int64) {
	if w.err != nil {
		return
	}
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(b, value)
	_, err := w.buffer.Write(b[0:n])
	if err != nil {
		w.err = err
	}

}

func (w *Writer) UUID(value UUID) {
	if w.err != nil {
		return
	}
	_, err := w.buffer.Write(value[:])
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Float64(value float64) {
	if w.err != nil {
		return
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(value))
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) String(value string) {
	w.Int16(int16(len(value)))
	if w.err != nil {
		return
	}

	b := make([]byte, len(value))
	i := 0
	for _, r := range value {
		n := utf8.EncodeRune(b[i:], r)
		if n == 0 {
			w.err = NewProtocolException("invalid_rune", "Rune could not be encoded")
			return
		}
		i += n
	}

	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) CompactString(value string) {
	w.Varlong(int64(len(value)))
	if w.err != nil {
		return
	}

	b := make([]byte, len(value))
	i := 0
	for _, r := range value {
		n := utf8.EncodeRune(b[i:], r)
		if n == 0 {
			w.err = NewProtocolException("invalid_rune", "Rune could not be encoded")
			return
		}
		i += n
	}
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) NullableString(value NullableString) {
	if value.IsNull {
		w.Int16(-1)
		return
	}

	w.Int16(int16(len(value.Value)))
	if w.err != nil {
		return
	}

	b := make([]byte, len(value.Value))
	i := 0
	for _, r := range value.Value {
		n := utf8.EncodeRune(b[i:], r)
		if n == 0 {
			w.err = NewProtocolException("invalid_rune", "Rune could not be encoded")
			return
		}
		i += n
	}

	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) CompactNullableString(value NullableString) {
	if value.IsNull {
		w.Varlong(0)
		return
	}
	w.Varlong(int64(len(value.Value)))
	if w.err != nil {
		return
	}
	b := make([]byte, len(value.Value))
	i := 0
	for _, r := range value.Value {
		n := utf8.EncodeRune(b[i:], r)
		if n == 0 {
			w.err = NewProtocolException("invalid_rune", "Rune could not be encoded")
			return
		}
		i += n
	}
	_, err := w.buffer.Write(b)
	if err != nil {
		w.err = err
	}
}

func (w *Writer) Bytes(value []byte) {
	w.Int32(int32(len(value)))
	if w.err != nil {
		return
	}

	n, err := w.buffer.Write(value)
	if err != nil {
		w.err = err
		return
	}

	if n != len(value) {
		w.err = NewProtocolException("invalid_binary_data", "Binary data could not be written")
	}
}

func (w *Writer) VarintBytes(value []byte) {
	w.Varlong(int64(len(value)))
	if w.err != nil {
		return
	}
	n, err := w.buffer.Write(value)
	if err != nil {
		w.err = err
		return
	}

	if n != len(value) {
		w.err = NewProtocolException("invalid_binary_data", "Binary data could not be written")
	}
}

func (w *Writer) CompactBytes(value []byte) {
	w.Uvarint(uint64(len(value)))
	if w.err != nil {
		return
	}
	n, err := w.buffer.Write(value)
	if err != nil {
		w.err = err
		return
	}
	if n != len(value) {
		w.err = NewProtocolException("invalid_binary_data", "Binary data could not be written")
	}
}

func (w *Writer) NullableBytes(value []byte) {
	if value == nil {
		w.Int32(int32(-1))
		return
	}
	w.Int32(int32(len(value)) + 1)
	if w.err != nil {
		return
	}
	n, err := w.buffer.Write(value)
	if err != nil {
		w.err = err
		return
	}
	if n != len(value) {
		w.err = NewProtocolException("invalid_binary_data", "Binary data could not be written")
	}
}

func (w *Writer) CompactNullableBytes(value []byte) {
	if value == nil {
		w.Uvarint(uint64(0))
		return
	}
	w.Uvarint(uint64(len(value) + 1))
	if w.err != nil {
		return
	}
	n, err := w.buffer.Write(value)
	if err != nil {
		w.err = err
		return
	}
	if n != len(value) {
		w.err = NewProtocolException("invalid_binary_data", "Binary data could not be written")
	}
}

func (w *Writer) RawBytes(value []byte) {
	n, err := w.buffer.Write(value)
	if err != nil {
		w.err = err
		return
	}
	if n != len(value) {
		w.err = NewProtocolException("invalid_binary_data", "Binary data could not be written")
	}
}

func (w *Writer) Data() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.buffer.Bytes(), nil
}

func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}

	data := w.buffer.Bytes()

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(data)))
	_, err := w.output.Write(b)
	if err != nil {
		return err
	}

	_, err = w.output.Write(data)
	if err != nil {
		return err
	}

	return nil
}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func (w *Writer) CRC32IEEE(data []byte) {
	w.Uint32(crc32.ChecksumIEEE(data))
}

func (w *Writer) CRC32Castagnoli(data []byte) {
	w.Uint32(crc32.Checksum(data, castagnoliTable))
}
