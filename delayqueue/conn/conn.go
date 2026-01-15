package conn

import (
	"sync"

	"github.com/beanstalkd/go-beanstalk"
)

type Conn struct {
	Endpoint string
	Tube     string
	Conn     *beanstalk.Conn

	lock sync.RWMutex
}

func NewConn(endpoint string, tube string) *Conn {
	return &Conn{
		Endpoint: endpoint,
		Tube:     tube,
	}
}

func (c *Conn) Close() error {
	c.lock.Lock()
	conn := c.Conn
	c.Conn = nil
	defer c.lock.Unlock()

	if conn != nil {
		return conn.Close()
	}

	return nil
}

func (c *Conn) Get() (*beanstalk.Conn, error) {
	c.lock.RLock()
	conn := c.Conn
	c.lock.RUnlock()

	if conn != nil {
		return conn, nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	c.Conn, err = beanstalk.Dial("tcp", c.Endpoint)
	if err != nil {
		return nil, err
	}

	c.Conn.Tube.Name = c.Tube
	c.Conn.TubeSet.Name[c.Tube] = true

	return c.Conn, err
}

func (c *Conn) Reset() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Conn != nil {
		c.Conn.Close()
		c.Conn = nil
	}
}
