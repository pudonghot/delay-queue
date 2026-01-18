package consumer

import (
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	c := NewConsumer(
		[]Cfg{
			{
				Endpoint: "172.16.8.11:11300",
				Tube:     "TEST_TUBE",
			},
			{
				Endpoint: "172.16.8.11:11300",
				Tube:     "TEST_TUBE2",
			},
		},
		time.Second*6,
	)

	release := atomic.Bool{}

	c.OnMessage(func(meta EventMata, data []byte) *MsgConsumeResult {
		log.Printf("endpoint [%s] tube [%s] message [%s].", meta.Endpoint, meta.Tube, string(data))
		if !release.Load() {
			log.Printf("Release message.")
			release.Store(true)
			return &MsgConsumeResult{
				ReleaseDelay: time.Second * 6,
			}
		}
		return nil
	})
	c.Start()
	done := make(chan string, 1)
	<-done
}
