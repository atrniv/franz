package protocol

import (
	"bufio"
	"encoding/binary"
	"io"
)

type TCPMessage []byte

func NewTCPMessage(src io.Reader) (TCPMessage, error) {
	reader := bufio.NewReader(src)

	// Read the message size
	b := make([]byte, 4)
	n, err := reader.Read(b)
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, NewProtocolException("invalid_size", "Invalid size provided for this field")
	}
	size := binary.BigEndian.Uint32(b)
	if err != nil {
		return nil, err
	}

	// Allocate space and read the data from network
	m := make([]byte, 0, size)
	for l := uint32(0); l < size; l++ {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		m = append(m, b)
	}
	return TCPMessage(m), nil
}

func (m TCPMessage) Write(w io.Writer) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(m)))
	_, err := w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write(m)
	if err != nil {
		return err
	}
	return nil
}
