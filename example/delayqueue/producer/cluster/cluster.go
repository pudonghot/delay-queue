package main

import (
	"fmt"
	"github.com/zeromicro/go-queue/delayqueue/consumer"
	"github.com/zeromicro/go-queue/delayqueue/producer"
	"strconv"
	"time"
)

func main() {
	producer := producer.NewProducer([]consumer.Cfg{
		{
			Endpoint: "172.16.8.11:11300",
			Tube:     "TEST_TUBE",
		},
		{
			Endpoint: "172.16.8.11:11300",
			Tube:     "TEST_TUBE2",
		},
	})

	for i := 1000; i < 1005; i++ {
		_, err := producer.Delay([]byte("消息："+strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		}
	}
}
