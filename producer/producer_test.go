package producer

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	send("TEST_TUBE")
	send("TEST_TUBE2")
}

func send(tube string) {
	producer := NewProducerNode("172.16.8.11:11300", tube)

	var removeId uint64
	for i := 1000; i < 1005; i++ {
		msgId, err := producer.Delay([]byte("消息："+strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Message: %d\n", msgId)
		removeId = msgId
	}
	producer.Remove(removeId)

	producer.Put([]byte("立刻，马上"))
}
