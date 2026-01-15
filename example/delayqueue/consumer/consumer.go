package main

import (
	"fmt"
	"github.com/zeromicro/go-queue/delayqueue"
)

func main() {
	consumer := delayqueue.NewConsumer(delayqueue.DqConf{
		Beanstalks: []delayqueue.Beanstalk{
			{
				Endpoint: "172.16.8.11:11300",
				Tube:     "TEST_TUBE",
			},
			{
				Endpoint: "172.16.8.11:11300",
				Tube:     "TEST_TUBE2",
			},
		},
	})
	consumer.Consume(func(body []byte) {
		fmt.Println(string(body))
	})
}
