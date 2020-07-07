package protocol

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
)

type UUID [16]byte

func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}

func NewUUID() UUID {
	u := uuid.NewV1()
	return UUID(u)
}

type NullableString struct {
	IsValid bool
	IsNull  bool
	Value   string
}

var EmptyNullableString = NullableString{}

type TagField struct {
	Type   uint64
	Length uint64
	Data   []byte
}
