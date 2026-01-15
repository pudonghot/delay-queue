package consumer

import (
	"github.com/pudonghot/delay-queue/delayqueue/util"
	log "github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/service"
)

type MessageListener func(meta EventMata, data []byte)

type Consumer interface {
	OnMessage(listener MessageListener)
}
type Cluster struct {
	nodes []*Node
}

func NewConsumer(cfgs []Cfg) Consumer {
	var nodes []*Node
	for _, node := range cfgs {
		nodes = append(nodes, newConsumerNode(node.Endpoint, node.Tube))
	}
	return &Cluster{
		nodes: nodes,
	}
}

func (c *Cluster) OnMessage(listener MessageListener) {
	group := service.NewServiceGroup()
	for _, node := range c.nodes {
		group.Add(Service{
			node: node,
			listener: func(_ EventMata, data []byte) {
				msg, err := util.Unwrap(data)
				if err != nil {
					log.Errorf("unwrap message error: %v\n", err)
					return
				}
				listener(EventMata{Endpoint: node.conn.Endpoint, Tube: node.conn.Tube}, msg)
			},
		})
	}
	group.Start()
}
