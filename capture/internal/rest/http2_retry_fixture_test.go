package rest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type peerFrame struct {
	kind    byte
	flags   byte
	stream  uint32
	payload []byte
}

func readPeerFrame(reader io.Reader) (peerFrame, error) {
	var header [9]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return peerFrame{}, err
	}
	length := int(header[0])<<16 | int(header[1])<<8 | int(header[2])
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return peerFrame{}, err
	}
	return peerFrame{header[3], header[4], binary.BigEndian.Uint32(header[5:]) & 0x7fffffff, payload}, nil
}

func writePeerFrame(writer io.Writer, kind, flags byte, stream uint32, payload []byte) error {
	header := []byte{byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)), kind, flags, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[5:], stream)
	_, err := io.Copy(writer, bytes.NewReader(append(header, payload...)))
	return err
}

type nativeH2Peer struct {
	listener    net.Listener
	certificate *x509.Certificate
	calls       atomic.Int32
	connections atomic.Int32
	stopped     atomic.Bool
	holdSecond  bool
	releaseHold chan struct{}
	holdSeen    chan struct{}
	once        sync.Once
	mu          sync.Mutex
	conns       []net.Conn
	workers     sync.WaitGroup
	t           *testing.T
}

func newNativeH2Peer(t *testing.T, holdSecond bool) *nativeH2Peer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	secure := tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"h2"}})
	peer := &nativeH2Peer{listener: secure, certificate: certificate, holdSecond: holdSecond, releaseHold: make(chan struct{}), holdSeen: make(chan struct{}), t: t}
	peer.workers.Go(func() {
		for {
			conn, err := secure.Accept()
			if err != nil {
				if !peer.stopped.Load() {
					t.Error(err)
				}
				return
			}
			peer.connections.Add(1)
			peer.mu.Lock()
			if peer.stopped.Load() {
				peer.mu.Unlock()
				if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Error(err)
				}
				return
			}
			peer.conns = append(peer.conns, conn)
			peer.mu.Unlock()
			peer.workers.Go(func() {
				defer func() {
					if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !peer.stopped.Load() {
						t.Error(err)
					}
				}()
				if err := peer.serve(conn); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !peer.stopped.Load() {
					t.Error(err)
				}
			})
		}
	})
	t.Cleanup(func() {
		peer.stopped.Store(true)
		peer.once.Do(func() { close(peer.releaseHold) })
		if err := secure.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
		peer.mu.Lock()
		for _, conn := range peer.conns {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Error(err)
			}
		}
		peer.mu.Unlock()
		peer.workers.Wait()
	})
	return peer
}

func (p *nativeH2Peer) serve(conn net.Conn) error {
	var preface [24]byte
	if _, err := io.ReadFull(conn, preface[:]); err != nil {
		return err
	}
	if string(preface[:]) != "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" {
		return fmt.Errorf("native peer did not negotiate HTTP/2")
	}
	var writes sync.Mutex
	write := func(kind, flags byte, stream uint32, payload []byte) error {
		writes.Lock()
		defer writes.Unlock()
		return writePeerFrame(conn, kind, flags, stream, payload)
	}
	if err := write(4, 0, 0, nil); err != nil {
		return err
	}
	for {
		frame, err := readPeerFrame(conn)
		if err != nil {
			return err
		}
		switch frame.kind {
		case 4:
			if frame.flags&1 == 0 {
				if err := write(4, 1, 0, nil); err != nil {
					return err
				}
			}
		case 6:
			if frame.flags&1 == 0 {
				if err := write(6, 1, 0, frame.payload); err != nil {
					return err
				}
			}
		case 7:
			return nil
		case 1:
			if frame.flags&4 == 0 {
				return fmt.Errorf("probe request headers unexpectedly fragmented")
			}
			number := p.calls.Add(1)
			if p.holdSecond && number == 2 {
				close(p.holdSeen)
				p.workers.Go(func() {
					<-p.releaseHold
					if err := write(1, 5, frame.stream, []byte{0x88}); err != nil && !p.stopped.Load() {
						p.t.Error(err)
					}
				})
				continue
			}
			refused := number == 2
			if p.holdSecond {
				refused = number == 3
			}
			if refused {
				if err := write(3, 0, frame.stream, []byte{0, 0, 0, 7}); err != nil {
					return err
				}
			} else if err := write(1, 5, frame.stream, []byte{0x88}); err != nil {
				return err
			}
		}
	}
}
