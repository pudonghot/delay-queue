package main

import (
	consumer2 "github.com/pudonghot/delay-queue/consumer"
	log "github.com/sirupsen/logrus"
)

func main() {
	c := consumer2.NewConsumer(
		[]consumer2.Cfg{
			{
				Endpoint: "172.16.8.11:11300",
				Tube:     "TEST_TUBE",
			},
			{
				Endpoint: "172.16.8.11:11300",
				Tube:     "TEST_TUBE2",
			},
		},
	)

	c.OnMessage(func(meta consumer2.EventMata, data []byte) {
		log.Printf("endpoint [%s] tube [%s] message [%s].", meta.Endpoint, meta.Tube, string(data))
	})
}
