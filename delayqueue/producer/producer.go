package producer

import (
	"github.com/zeromicro/go-queue/delayqueue/consumer"
	"github.com/zeromicro/go-queue/delayqueue/util"
	"math/rand"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/errorx"
	"github.com/zeromicro/go-zero/core/fx"
	"github.com/zeromicro/go-zero/core/lang"
)

const (
	replicaNodes    = 3
	minWrittenNodes = 2
)

type (
	Producer interface {
		Put(body []byte) (string, error)
		At(body []byte, at time.Time) (string, error)
		Delay(body []byte, delay time.Duration) (string, error)
		Revoke(ids string) error
		Close() error

		at(body []byte, at time.Time) (string, error)
		delay(body []byte, delay time.Duration) (string, error)
	}

	cluster struct {
		nodes []Producer
	}
)

var rng *rand.Rand

func init() {
	source := rand.NewSource(time.Now().UnixNano())
	rng = rand.New(source)
}

func NewProducer(beanstalks []consumer.Cfg) Producer {
	if len(beanstalks) < minWrittenNodes {
		log.Fatalf("nodes must be equal or greater than %d", minWrittenNodes)
	}

	var nodes []Producer
	producers := make(map[string]lang.PlaceholderType)
	for _, node := range beanstalks {
		if _, ok := producers[node.Endpoint]; ok {
			log.Fatal("all node endpoints must be different")
		}

		producers[node.Endpoint] = lang.Placeholder
		nodes = append(nodes, NewProducerNode(node.Endpoint, node.Tube))
	}

	return &cluster{nodes: nodes}
}

func (p *cluster) Put(body []byte) (string, error) {
	return p.Delay(body, 0)
}

func (p *cluster) At(data []byte, at time.Time) (string, error) {
	msg, err := util.Wrap(data, at)
	if err != nil {
		return "", nil
	}
	return p.at(msg, at)
}

func (p *cluster) Close() error {
	var be errorx.BatchError
	for _, node := range p.nodes {
		if err := node.Close(); err != nil {
			be.Add(err)
		}
	}
	return be.Err()
}

func (p *cluster) Delay(data []byte, delay time.Duration) (string, error) {
	msg, err := util.Wrap(data, time.Now().Add(delay))
	if err != nil {
		return "", err
	}
	return p.delay(msg, delay)
}

func (p *cluster) Revoke(ids string) error {
	var be errorx.BatchError

	fx.From(func(source chan<- interface{}) {
		for _, node := range p.nodes {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(Producer)
		return node.Revoke(ids)
	}).ForEach(func(item interface{}) {
		if item != nil {
			be.Add(item.(error))
		}
	})

	return be.Err()
}

func (p *cluster) at(body []byte, at time.Time) (string, error) {
	return p.insert(func(node Producer) (string, error) {
		return node.at(body, at)
	})
}

func (p *cluster) cloneNodes() []Producer {
	return append([]Producer(nil), p.nodes...)
}

func (p *cluster) delay(body []byte, delay time.Duration) (string, error) {
	return p.insert(func(node Producer) (string, error) {
		return node.delay(body, delay)
	})
}

func (p *cluster) getWriteNodes() []Producer {
	if len(p.nodes) <= replicaNodes {
		return p.nodes
	}

	nodes := p.cloneNodes()
	rng.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return nodes[:replicaNodes]
}

func (p *cluster) insert(fn func(node Producer) (string, error)) (string, error) {
	type idErr struct {
		id  string
		err error
	}
	var ret []idErr
	fx.From(func(source chan<- interface{}) {
		for _, node := range p.getWriteNodes() {
			source <- node
		}
	}).Map(func(item interface{}) interface{} {
		node := item.(Producer)
		id, err := fn(node)
		return idErr{
			id:  id,
			err: err,
		}
	}).ForEach(func(item interface{}) {
		ret = append(ret, item.(idErr))
	})

	var ids []string
	var be errorx.BatchError
	for _, val := range ret {
		if val.err != nil {
			be.Add(val.err)
		} else {
			ids = append(ids, val.id)
		}
	}

	jointId := strings.Join(ids, idSep)
	if len(ids) >= minWrittenNodes {
		return jointId, nil
	}

	if err := p.Revoke(jointId); err != nil {
		log.Error(err)
	}

	return "", be.Err()
}
