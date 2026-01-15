package delayqueue

import (
	log "github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/service"
	"time"
)

const (
	tolerance = time.Minute * 30
)

type (
	Consume func(body []byte)

	Consumer interface {
		Consume(consume Consume)
	}

	consumerCluster struct {
		nodes []*consumerNode
	}
)

func NewConsumer(c DqConf) Consumer {
	var nodes []*consumerNode
	for _, node := range c.Beanstalks {
		nodes = append(nodes, newConsumerNode(node.Endpoint, node.Tube))
	}
	return &consumerCluster{
		nodes: nodes,
	}
}

func (c *consumerCluster) Consume(consume Consume) {
	consumerFn := func(body []byte) {
		taskBody, ok := unwrap(body)
		if !ok {
			log.Errorf("discarded: %q", string(body))
			return
		}
		consume(taskBody)
	}

	group := service.NewServiceGroup()
	for _, node := range c.nodes {
		group.Add(consumeService{
			c:       node,
			consume: consumerFn,
		})
	}
	group.Start()
}
