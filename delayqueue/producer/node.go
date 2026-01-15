package producer

import (
	"errors"
	"fmt"
	"github.com/zeromicro/go-queue/delayqueue/conn"
	"github.com/zeromicro/go-queue/delayqueue/util"
	"strconv"
	"strings"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	log "github.com/sirupsen/logrus"
)

var ErrTimeBeforeNow = errors.New("can't schedule task to past time")

const (
	PriHigh   = 1
	PriNormal = 2
	PriLow    = 3

	defaultTimeToRun = time.Second * 5
	reserveTimeout   = time.Second * 5

	idSep = ","
)

type node struct {
	endpoint string
	tube     string
	conn     *conn.Conn
}

func NewProducerNode(endpoint, tube string) Producer {
	return &node{
		endpoint: endpoint,
		tube:     tube,
		conn:     conn.NewConn(endpoint, tube),
	}
}

func (p *node) Put(body []byte) (string, error) {
	return p.Delay(body, 0)
}

func (p *node) At(body []byte, at time.Time) (string, error) {
	msg, err := util.Wrap(body, at)
	if err != nil {
		return "", err
	}
	return p.at(msg, at)
}

func (p *node) Close() error {
	return p.conn.Close()
}

func (p *node) Delay(data []byte, delay time.Duration) (string, error) {
	msg, err := util.Wrap(data, time.Now().Add(delay))
	if err != nil {
		return "", err
	}
	return p.delay(msg, delay)
}

func (p *node) Revoke(ids string) error {
	for _, id := range strings.Split(ids, idSep) {
		fields := strings.Split(id, "/")
		if len(fields) < 3 {
			continue
		}
		if fields[0] != p.endpoint || fields[1] != p.tube {
			continue
		}

		conn, err := p.conn.Get()
		if err != nil {
			return err
		}

		n, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return err
		}

		return conn.Delete(n)
	}

	// if not in this beanstalk, ignore
	return nil
}

func (p *node) at(data []byte, at time.Time) (string, error) {
	now := time.Now()
	if at.Before(now) {
		return "", ErrTimeBeforeNow
	}

	duration := at.Sub(now)
	return p.delay(data, duration)
}

func (p *node) delay(data []byte, delay time.Duration) (string, error) {
	conn, err := p.conn.Get()
	if err != nil {
		return "", err
	}

	id, err := conn.Put(data, PriNormal, delay, defaultTimeToRun)
	if err == nil {
		return fmt.Sprintf("%s/%s/%d", p.endpoint, p.tube, id), nil
	}

	// the error can only be beanstalk.NameError or beanstalk.ConnError
	// just return when the error is beanstalk.NameError, don't reset
	var connError beanstalk.ConnError
	switch {
	case errors.As(err, &connError):
		switch {
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
		default:
			// beanstalk.ErrOOM, beanstalk.ErrTimeout, beanstalk.ErrUnknown and other errors
			p.conn.Reset()
		}
	default:
		log.Error(err)
	}

	return "", err
}
