package domain

import "fmt"

type KafkaException struct {
	Code        int16  `json:"code"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func NewKafkaException(code int16, name string, description string) KafkaException {
	return KafkaException{
		Code:        code,
		Name:        name,
		Description: description,
	}
}

func (e KafkaException) Error() string {
	return fmt.Sprintf("(%s: %d) %s", e.Name, e.Code, e.Description)
}
