package clientpool

import (
	"crypto/tls"
	"net"
	"time"
	"errors"

	"github.com/cloudfoundry/gosteno"
)

const timeout = 1 * time.Second

type TLSClient struct {
	address   string
	tlsConfig *tls.Config
	logger    *gosteno.Logger

	lock chan struct{}
	conn net.Conn
}

func NewTLSClient(logger *gosteno.Logger, address string, tlsConfig *tls.Config) *TLSClient {
	return &TLSClient{
		lock: make(chan struct{}, 1),
		address:   address,
		tlsConfig: tlsConfig,
		logger:    logger,
	}
}

func (c *TLSClient) Connect() error {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", c.address, c.tlsConfig)
	if err != nil {
		c.logger.Warnd(map[string]interface{}{
			"error":   err,
			"address": c.address,
		}, "Failed to connect over TLS")
		return err
	}
	c.conn = conn
	return nil
}

func (c *TLSClient) Scheme() string {
	return "tls"
}

func (c *TLSClient) Address() string {
	return c.address
}

func (c *TLSClient) Close() error {
	c.lock <- struct{}{}
	defer func() {<-c.lock}()
	if c.conn == nil {
		return nil
	}

	conn := c.conn
	c.conn = nil
	if err := conn.Close(); err != nil {
		c.logger.Warnf("Error closing TLS connection: %v", err)
		return err
	}

	return nil
}

func (c *TLSClient) logError(err error) {
	c.logger.Errord(map[string]interface{}{
		"scheme":  c.Scheme(),
		"address": c.Address(),
		"error":   err.Error(),
	}, "TLSClient: streaming error")
}

func (c *TLSClient) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	select {
	case c.lock <- struct{}{}:
	default:
		return 0, errors.New("TLS client already in use")
	}
	defer func() {<-c.lock}()
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			c.logError(err)
			return 0, err
		}
	}
	conn := c.conn

	written, err := conn.Write(data)
	if err != nil {
		c.logError(err)
	}
	return written, err
}
