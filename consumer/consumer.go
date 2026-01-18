package consumer

import (
	"github.com/pudonghot/delay-queue/util"
	log "log/slog"
	"time"
)

type Cfg struct {
	Endpoint string
	Tube     string
}

type MsgConsumeResult struct {
	ReleaseDelay time.Duration
}

type MessageListener func(meta EventMata, data []byte) *MsgConsumeResult

type Consumer interface {
	OnMessage(listener MessageListener)
	Start()
	Stop()
}

type Cluster struct {
	nodes     []*Node
	listeners []MessageListener
}

func NewConsumer(cfgs []Cfg, reserveTimeout time.Duration) Consumer {
	var nodes []*Node
	for _, node := range cfgs {
		nodes = append(nodes, newConsumerNode(node.Endpoint, node.Tube, reserveTimeout))
	}
	return &Cluster{
		nodes:     nodes,
		listeners: make([]MessageListener, 0),
	}
}

func (c *Cluster) OnMessage(listener MessageListener) {
	c.listeners = append(c.listeners, listener)
}

func (c *Cluster) Start() {
	for _, node := range c.nodes {
		for _, listener := range c.listeners {
			go func() {
				node.listen(
					func(_ EventMata, data []byte) *MsgConsumeResult {
						msg, err := util.Unwrap(data)
						if err != nil {
							log.Error("Unwrap message err", log.Any("error", err))
							return nil
						}
						return listener(EventMata{Endpoint: node.conn.Endpoint, Tube: node.conn.Tube}, msg)
					})
			}()
		}
	}
}

func (c *Cluster) Stop() {
	for _, node := range c.nodes {
		node.stop()
	}
}
