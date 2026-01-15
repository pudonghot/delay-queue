package consumer

import (
	log "github.com/sirupsen/logrus"
	"testing"
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
	)

	c.OnMessage(func(meta EventMata, data []byte) {
		log.Printf("endpoint [%s] tube [%s] message [%s].", meta.Endpoint, meta.Tube, string(data))
	})
}
