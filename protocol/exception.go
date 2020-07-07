package protocol

import (
	"fmt"

	"github.com/facebookgo/stack"
)

// ProtocolException represents an application level exception
type ProtocolException struct {
	Name    string      `json:"name"`
	Message string      `json:"message"`
	Stack   stack.Stack `json:"stack"`
}

func (e ProtocolException) Error() string {
	return fmt.Sprintf("[%s] %s", e.Name, e.Message)
}

// NewProtocolException returns a new application level exception
func NewProtocolException(name string, message string, params ...interface{}) error {
	err := ProtocolException{
		Name:    name,
		Message: fmt.Sprintf(message, params...),
		Stack:   stack.Callers(1),
	}
	return err
}
