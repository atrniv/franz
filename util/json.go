package util

import (
	"encoding/json"
	"fmt"
)

func Debug(marker string, value interface{}) {
	data, err := json.MarshalIndent(value, "    ", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s: %s\n", marker, string(data))
}
