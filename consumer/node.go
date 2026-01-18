package consumer

import (
	"errors"
	"github.com/pudonghot/delay-queue/conn"
	"sync/atomic"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	log "log/slog"
)

type Node struct {
	conn           *conn.Conn
	on             *atomic.Bool
	reserveTimeout time.Duration
}

type EventMata struct {
	Endpoint string
	Tube     string
}

func newConsumerNode(endpoint string, tube string, reserveTimeout time.Duration) *Node {
	on := atomic.Bool{}
	on.Store(true)
	return &Node{
		conn:           conn.NewConn(endpoint, tube),
		on:             &on,
		reserveTimeout: reserveTimeout,
	}
}

func (node *Node) stop() {
	node.on.Store(false)
}

func (node *Node) listen(listener MessageListener) {
	for node.on.Load() {
		conn, err := node.conn.Get()
		if err != nil {
			log.Error("Beanstalk conn err", log.Any("error", err))
			time.Sleep(time.Second)
			continue
		}

		// because getting conn takes at most one second, reserve tasks at most 5 seconds,
		// if don't check on/off here, the conn might not be closed due to
		// graceful shutdown waits at most 5.5 seconds.
		if !node.on.Load() {
			break
		}

		id, body, err := conn.Reserve(node.reserveTimeout)
		if err == nil {
			result := listener(EventMata{
				Endpoint: node.conn.Endpoint,
				Tube:     node.conn.Tube,
			}, body)

			if result != nil {
				delay := result.ReleaseDelay
				if delay >= 0 {
					conn.Release(id, 2, delay)
					continue
				}
			}

			conn.Delete(id)
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
				log.Error("Beanstalk consume err", log.Any("error", err))
			default:
				// beanstalk.ErrOOM, beanstalk.ErrUnknown and other errors
				log.Error("Beanstalk unknown err, conn will be reset", log.Any("error", err))
				node.conn.Reset()
				time.Sleep(time.Second)
			}
		default:
			log.Error("Beanstalk consume err", log.Any("error", err))
		}
	}

	if err := node.conn.Close(); err != nil {
		log.Error("Beanstalk close err", log.Any("error", err))
	}
}
