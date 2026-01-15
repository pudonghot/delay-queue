package consumer

import (
	"errors"
	"github.com/pudonghot/delay-queue/delayqueue/conn"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	log "github.com/sirupsen/logrus"
	"github.com/zeromicro/go-zero/core/syncx"
)

const (
	reserveTimeout = time.Second * 6
)

type Node struct {
	conn *conn.Conn
	on   *syncx.AtomicBool
}

type Service struct {
	node     *Node
	listener MessageListener
}

type EventMata struct {
	Endpoint string
	Tube     string
}

func newConsumerNode(endpoint string, tube string) *Node {
	return &Node{
		conn: conn.NewConn(endpoint, tube),
		on:   syncx.ForAtomicBool(true),
	}
}

func (c *Node) dispose() {
	c.on.Set(false)
}

func (c *Node) consume(listener MessageListener) {
	for c.on.True() {
		conn, err := c.conn.Get()
		if err != nil {
			log.Error(err)
			time.Sleep(time.Second)
			continue
		}

		// because getting conn takes at most one second, reserve tasks at most 5 seconds,
		// if don't check on/off here, the conn might not be closed due to
		// graceful shutdown waits at most 5.5 seconds.
		if !c.on.True() {
			break
		}

		id, body, err := conn.Reserve(reserveTimeout)
		if err == nil {
			conn.Delete(id)
			listener(EventMata{
				Endpoint: c.conn.Endpoint,
				Tube:     c.conn.Tube,
			}, body)
			continue
		}

		// the error can only be beanstalk.NameError or beanstalk.ConnError
		var connError beanstalk.ConnError
		switch {
		case errors.As(err, &connError):
			switch {
			case errors.Is(connError.Err, beanstalk.ErrTimeout):
				// timeout error on timeout, just continue the loop
			case
				errors.Is(connError.Err, beanstalk.ErrBadChar),
				errors.Is(connError.Err, beanstalk.ErrBadFormat),
				errors.Is(connError.Err, beanstalk.ErrBuried),
				errors.Is(connError.Err, beanstalk.ErrDeadline),
				errors.Is(connError.Err, beanstalk.ErrDraining),
				errors.Is(connError.Err, beanstalk.ErrEmpty),
				errors.Is(connError.Err, beanstalk.ErrInternal),
				errors.Is(connError.Err, beanstalk.ErrJobTooBig),
				errors.Is(connError.Err, beanstalk.ErrNoCRLF),
				errors.Is(connError.Err, beanstalk.ErrNotFound),
				errors.Is(connError.Err, beanstalk.ErrNotIgnored),
				errors.Is(connError.Err, beanstalk.ErrTooLong):
				// won't reset
				log.Error(err)
			default:
				// beanstalk.ErrOOM, beanstalk.ErrUnknown and other errors
				log.Error(err)
				c.conn.Reset()
				time.Sleep(time.Second)
			}
		default:
			log.Error(err)
		}
	}

	if err := c.conn.Close(); err != nil {
		log.Error(err)
	}
}

func (cs Service) Start() {
	cs.node.consume(cs.listener)
}

func (cs Service) Stop() {
	cs.node.dispose()
}
