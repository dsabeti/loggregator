package clientpool_test

import (
	"crypto/tls"
	"doppler/listeners"
	"metron/clientpool"
	"net"
	"time"

	"github.com/cloudfoundry/gosteno"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCP Client", func() {
	var (
		logger   *gosteno.Logger
		listener net.Listener
		connChan chan net.Conn

		client          *clientpool.TCPClient
		tlsClientConfig *tls.Config
		tlsServerConfig *tls.Config
		err             error
	)

	JustBeforeEach(func() {
		logger = gosteno.NewLogger("test")

		var err error
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		if tlsServerConfig != nil {
			listener = tls.NewListener(listener, tlsServerConfig)
		}
		Expect(err).NotTo(HaveOccurred())

		client = clientpool.NewTCPClient(logger, listener.Addr().String(), tlsClientConfig)
		Expect(client).ToNot(BeNil())
	})

	AfterEach(func() {
		listener.Close()
		if client != nil {
			client.Close()
		}
	})

	Describe("with a TLS configuration", func() {
		BeforeEach(func() {
			tlsServerConfig, err = listeners.NewTLSConfig("fixtures/server.crt", "fixtures/server.key", "fixtures/loggregator-ca.crt")
			Expect(err).NotTo(HaveOccurred())

			tlsClientConfig, err = listeners.NewTLSConfig("fixtures/client.crt", "fixtures/client.key", "fixtures/loggregator-ca.crt")
			Expect(err).NotTo(HaveOccurred())
			tlsClientConfig.ServerName = "doppler"
		})

		Describe("Connect", func() {
			var connErr error

			JustBeforeEach(func() {
				connChan = acceptTLSConnections(listener)
			})

			Context("with a valid TLSListener", func() {
				It("returns a connection without error", func() {
					connErr = client.Connect()
					Expect(connErr).NotTo(HaveOccurred())
					Eventually(connChan).Should(Receive())
				})
			})

			Context("without a TLSListener", func() {
				It("returns an error", func() {
					Expect(listener.Close()).ToNot(HaveOccurred())
					connErr = client.Connect()
					Expect(connErr).To(HaveOccurred())
					Consistently(connChan).ShouldNot(Receive())
				})
			})
		})

		Describe("Scheme", func() {
			It("returns tls", func() {
				Expect(client.Scheme()).To(Equal("tls"))
			})
		})

		Describe("Address", func() {
			It("returns the address", func() {
				Expect(client.Address()).To(Equal(listener.Addr().String()))
			})
		})

		Describe("Write", func() {
			var conn net.Conn

			JustBeforeEach(func() {
				connChan = acceptTLSConnections(listener)
				Expect(client.Connect()).ToNot(HaveOccurred())
				conn = <-connChan
			})

			It("sends data", func() {
				_, err := client.Write([]byte("abc"))
				Expect(err).NotTo(HaveOccurred())
				bytes := make([]byte, 10)
				n, err := conn.Read(bytes)
				Expect(err).NotTo(HaveOccurred())
				Expect(bytes[:n]).To(Equal([]byte("abc")))
			})

			Context("when write is called with an empty byte slice", func() {
				var writeErr error

				JustBeforeEach(func() {
					_, writeErr = client.Write([]byte{})
				})

				It("does not send", func() {
					Expect(writeErr).NotTo(HaveOccurred())

					bytes := make([]byte, 10)
					conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
					_, err := conn.Read(bytes)
					Expect(err).To(HaveOccurred())
					opErr := err.(*net.OpError)
					Expect(opErr.Timeout()).To(BeTrue())
				})
			})

			Context("when the connection is closed", func() {
				It("reconnects and sends", func() {
					Expect(client.Close()).ToNot(HaveOccurred())
					_, err := client.Write([]byte("abc"))
					Expect(err).NotTo(HaveOccurred())

					Eventually(connChan).Should(Receive(&conn))
					Expect(conn).NotTo(BeNil())
					bytes := make([]byte, 10)
					n, err := conn.Read(bytes)
					Expect(err).NotTo(HaveOccurred())
					Expect(bytes[:n]).To(Equal([]byte("abc")))
				})
			})
		})

		Describe("Close", func() {
			It("can be called multiple times", func() {
				done := make(chan struct{})
				go func() {
					client.Close()
					client.Close()
					close(done)
				}()
				Eventually(done).Should(BeClosed())
			})
		})
	})

	Describe("without a TLS configuration", func() {
		BeforeEach(func() {
			tlsServerConfig = nil
			tlsClientConfig = nil
		})

		Describe("Write", func() {
			var conn net.Conn
			JustBeforeEach(func() {
				connChan = acceptTCPConnections(listener)
				Expect(client.Connect()).ToNot(HaveOccurred())
				conn = <-connChan
			})

			It("sends data", func() {
				_, err := client.Write([]byte("abc"))
				Expect(err).NotTo(HaveOccurred())
				bytes := make([]byte, 10)
				n, err := conn.Read(bytes)
				Expect(err).NotTo(HaveOccurred())
				Expect(bytes[:n]).To(Equal([]byte("abc")))
			})
		})

		Describe("Scheme", func() {
			It("returns tcp", func() {
				Expect(client.Scheme()).To(Equal("tcp"))
			})
		})
	})
})

func acceptTLSConnections(listener net.Listener) chan net.Conn {
	connChan := make(chan net.Conn, 1)
	go func() {
		defer GinkgoRecover()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			err = conn.(*tls.Conn).Handshake()
			if err == nil {
				connChan <- conn
			}
		}
	}()
	return connChan
}

func acceptTCPConnections(listener net.Listener) chan net.Conn {
	connChan := make(chan net.Conn, 1)
	go func() {
		defer GinkgoRecover()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connChan <- conn
		}
	}()
	return connChan
}
