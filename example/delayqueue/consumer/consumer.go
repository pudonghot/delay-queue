package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/zeromicro/go-queue/delayqueue/consumer"
)

func main() {
	c := consumer.NewConsumer(
		[]consumer.Cfg{
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

	c.OnMessage(func(meta consumer.EventMata, body []byte) {
		log.Printf("endpoint [%s] tube [%s] message [%s].", meta.Endpoint, meta.Tube, string(body))
	})
}
