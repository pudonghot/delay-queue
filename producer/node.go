package producer

import (
	"errors"
	"fmt"
	"github.com/pudonghot/delay-queue/conn"
	"github.com/pudonghot/delay-queue/util"
	log "log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/beanstalkd/go-beanstalk"
)

var ErrTimeBeforeNow = errors.New("can't schedule task to past time")

const (
	PriHigh   = 1
	PriNormal = 2
	PriLow    = 3

	defaultTimeToRun = time.Second * 6
)

type Producer interface {
	Put(data []byte) (string, error)
	At(data []byte, at time.Time) (string, error)
	Delay(data []byte, delay time.Duration) (string, error)
	Revoke(id string) error
	Close() error

	delay(data []byte, delay time.Duration) (string, error)
}

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

	now := time.Now()
	if at.Before(now) {
		return "", ErrTimeBeforeNow
	}

	duration := at.Sub(now)
	return p.delay(msg, duration)
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

func (p *node) Revoke(id string) error {
	fields := strings.Split(id, "/")
	if len(fields) != 3 {
		return fmt.Errorf("invalid format of message id: %s", id)
	}

	if fields[0] != p.endpoint || fields[1] != p.tube {
		return fmt.Errorf("invalid message id: [%s]", id)
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

func (p *node) delay(data []byte, delay time.Duration) (string, error) {
	conn, err := p.conn.Get()
	if err != nil {
		log.Error("Beanstalk conn err", log.Any("error", err))
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
			log.Error("Beanstalk unknown err, conn will be reset.", log.Any("error", err))
			p.conn.Reset()
		}
	default:
		log.Error("Beanstalk put err", log.Any("error", err))
	}

	return "", err
}
