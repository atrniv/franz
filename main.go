package main

import (
	"os"
	"time"

	"github.com/atrniv/franz/domain"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
}

func main() {
	cluster := domain.NewCluster("kafka")
	err := cluster.StartBroker(":9092", nil, false)
	if err != nil {
		panic(err)
	}
}
