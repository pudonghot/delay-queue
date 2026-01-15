package main

import (
	"fmt"
	"github.com/pudonghot/delay-queue/delayqueue/producer"
	"strconv"
	"time"
)

func main() {
	send("TEST_TUBE")
	send("TEST_TUBE2")
}

func send(tube string) {
	producer := producer.NewProducerNode("172.16.8.11:11300", tube)

	var revokeId string
	for i := 1000; i < 1005; i++ {
		msgId, err := producer.Delay([]byte("消息："+strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Message: %s\n", msgId)
		revokeId = msgId
	}
	producer.Revoke(revokeId)

	producer.Put([]byte("立刻，马上"))
}
