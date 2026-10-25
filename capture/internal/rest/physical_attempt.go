package rest

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

type negotiatedTLS interface{ ConnectionState() tls.ConnectionState }

type physicalAdmission struct {
	gate     *budget.Gate
	priority int
	host     string
	buckets  []budget.Bucket
	ctx      context.Context
	cancel   context.CancelCauseFunc

	mu          sync.Mutex
	connections int
	lease       *hostLease
	failure     error
	done        sync.Once
}

func newPhysicalAdmission(request *http.Request, gate *budget.Gate, host string, buckets []budget.Bucket, priority int, lease *hostLease) (*http.Request, *physicalAdmission) {
	ctx, cancel := context.WithCancelCause(request.Context())
	admission := &physicalAdmission{gate: gate, priority: priority, host: host, buckets: buckets, ctx: ctx, cancel: cancel, lease: lease}
	trace := &httptrace.ClientTrace{GotConn: admission.connection}
	return request.Clone(httptrace.WithClientTrace(ctx, trace)), admission
}

func (a *physicalAdmission) reject(cause error, conn net.Conn) {
	a.cancel(cause)
	if conn != nil {
		secure, multiplexed := conn.(negotiatedTLS)
		if !multiplexed || secure.ConnectionState().NegotiatedProtocol != "h2" {
			cause = errors.Join(cause, conn.Close())
		}
	}
	a.mu.Lock()
	if a.failure == nil {
		a.failure = cause
	}
	a.mu.Unlock()
}

func (a *physicalAdmission) connection(info httptrace.GotConnInfo) {
	if info.Conn == nil {
		a.reject(errors.New("transport trace supplied no physical connection"), nil)
		return
	}
	a.mu.Lock()
	a.connections++
	first := a.connections == 1
	previous := a.lease
	if !first {
		a.lease = nil
	}
	a.mu.Unlock()
	if first {
		return
	}
	if previous != nil {
		previous.close()
	}
	if err := a.gate.AdmitBuckets(a.ctx, a.host, a.buckets, a.priority); err != nil {
		a.reject(err, info.Conn)
		return
	}
	release, err := a.gate.Enter(a.ctx, a.host, a.priority)
	if err != nil {
		a.reject(err, info.Conn)
		return
	}
	a.mu.Lock()
	a.lease = &hostLease{release: release}
	a.mu.Unlock()
}

func (a *physicalAdmission) rejection() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.failure
}

func (a *physicalAdmission) finish() {
	a.done.Do(func() {
		a.mu.Lock()
		lease := a.lease
		a.lease = nil
		a.mu.Unlock()
		if lease != nil {
			lease.close()
		}
		a.cancel(nil)
	})
}
