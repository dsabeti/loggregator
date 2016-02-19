package clientpool

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/cloudfoundry/gosteno"
)

const timeout = 1 * time.Second

type TCPClient struct {
	address   string
	tlsConfig *tls.Config
	logger    *gosteno.Logger

	lock   sync.Mutex
	scheme string
	conn   net.Conn
}

func NewTCPClient(logger *gosteno.Logger, address string, tlsConfig *tls.Config) *TCPClient {
	client := &TCPClient{
		address:   address,
		tlsConfig: tlsConfig,
		logger:    logger,
	}
	if tlsConfig == nil {
		client.scheme = "tcp"
	} else {
		client.scheme = "tls"
	}
	return client
}

func (c *TCPClient) Connect() error {
	var conn net.Conn
	var err error
	if c.tlsConfig == nil {
		conn, err = net.DialTimeout("tcp", c.address, timeout)
	} else {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", c.address, c.tlsConfig)
	}

	if err != nil {
		c.logger.Warnd(map[string]interface{}{
			"error":   err,
			"address": c.address,
		}, "Failed to connect over "+c.Scheme())
		return err
	}
	c.conn = conn
	return nil
}

func (c *TCPClient) Scheme() string {
	return c.scheme
}

func (c *TCPClient) Address() string {
	return c.address
}

func (c *TCPClient) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.conn == nil {
		return nil
	}

	conn := c.conn
	c.conn = nil
	if err := conn.Close(); err != nil {
		c.logger.Warnf("Error closing %s connection: %v", c.Scheme(), err)
		return err
	}

	return nil
}

func (c *TCPClient) logError(err error) {
	c.logger.Errord(map[string]interface{}{
		"scheme":  c.Scheme(),
		"address": c.Address(),
		"error":   err.Error(),
	}, "TCPClient: streaming error")
}

func (c *TCPClient) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	c.lock.Lock()
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			c.lock.Unlock()
			c.logError(err)
			return 0, err
		}
	}
	conn := c.conn
	c.lock.Unlock()

	written, err := conn.Write(data)
	if err != nil {
		c.logError(err)
	}
	return written, err
}
