package rest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

type hostLease struct {
	release func()
	once    sync.Once
}

func (l *hostLease) close() { l.once.Do(l.release) }

type initialAdmission struct {
	buckets []budget.Bucket
	lease   *hostLease
}

type httpAdmissionError struct{ cause error }

func (e *httpAdmissionError) Error() string { return e.cause.Error() }
func (e *httpAdmissionError) Unwrap() error { return e.cause }

type admittedBody struct {
	io.ReadCloser
	lease *hostLease
}

func (b *admittedBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if err != nil {
		b.lease.close()
	}
	return n, err
}

func (b *admittedBody) Close() error {
	err := b.ReadCloser.Close()
	b.lease.close()
	return err
}

type responseObservation struct{ retry bool }

type admittingTransport struct {
	base            http.RoundTripper
	gate            *budget.Gate
	priority        int
	initial         initialAdmission
	mu              sync.Mutex
	started         bool
	lastResponse    *http.Response
	lastObservation responseObservation
}

func (t *admittingTransport) reject(request *http.Request, lease *hostLease, cause error) (*http.Response, error) {
	if lease != nil {
		lease.close()
	}
	if request.Body != nil {
		cause = errors.Join(cause, request.Body.Close())
	}
	return nil, &httpAdmissionError{cause: cause}
}

func requestBudgetBody(request *http.Request) (*http.Request, []byte, error) {
	if strings.ToLower(request.URL.Hostname()) != "api.hyperliquid.xyz" || request.Body == nil || request.Body == http.NoBody {
		return request, nil, nil
	}
	data, readErr := io.ReadAll(request.Body)
	closeErr := request.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return request, nil, err
	}
	copy := request.Clone(request.Context())
	copy.Body = io.NopCloser(bytes.NewReader(data))
	return copy, data, nil
}

func (t *admittingTransport) RoundTrip(original *http.Request) (*http.Response, error) {
	t.mu.Lock()
	first := !t.started
	t.started = true
	t.mu.Unlock()
	var lease *hostLease
	if first {
		lease = t.initial.lease
	}
	request, body, err := requestBudgetBody(original)
	if err != nil {
		return t.reject(request, lease, err)
	}
	host := strings.ToLower(request.URL.Hostname())
	query, err := effectiveFetchParams(request.URL, nil, t.gate.QueryFields(host, request.URL.Path))
	if err != nil {
		return t.reject(request, lease, err)
	}
	buckets, err := t.gate.PlanFetch(request.Method, host, request.URL.Path, query, body)
	if err != nil {
		return t.reject(request, lease, err)
	}
	if first {
		if !slices.Equal(buckets, t.initial.buckets) {
			return t.reject(request, lease, fmt.Errorf("physical request differs from its initial admission"))
		}
	} else {
		if err := t.gate.AdmitBuckets(request.Context(), host, buckets, t.priority); err != nil {
			return t.reject(request, nil, err)
		}
		release, err := t.gate.Enter(request.Context(), host, t.priority)
		if err != nil {
			return t.reject(request, nil, err)
		}
		lease = &hostLease{release: release}
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		lease.close()
		return nil, err
	}
	if response == nil {
		lease.close()
		return nil, fmt.Errorf("HTTP transport returned no response")
	}
	retry, err := t.gate.Observe(request.Context(), host, response.StatusCode, response.Header)
	if err != nil {
		if response.Body != nil {
			err = errors.Join(err, response.Body.Close())
		}
		lease.close()
		return nil, &httpAdmissionError{cause: err}
	}
	if response.Body == nil {
		lease.close()
	} else {
		response.Body = &admittedBody{ReadCloser: response.Body, lease: lease}
	}
	t.mu.Lock()
	t.lastResponse = response
	t.lastObservation = responseObservation{retry: retry}
	t.mu.Unlock()
	return response, nil
}

func defaultPort(target *url.URL) string {
	if port := target.Port(); port != "" {
		return port
	}
	switch strings.ToLower(target.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}

func sameAuthority(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Hostname(), right.Hostname()) && defaultPort(left) == defaultPort(right)
}

func sameUserInfo(left, right *url.URL) bool {
	if left.User == nil || right.User == nil {
		return left.User == nil && right.User == nil
	}
	return left.User.String() == right.User.String()
}

func safeRedirectHeaders(source http.Header, userAgent string) http.Header {
	target := make(http.Header)
	for key, values := range source {
		switch http.CanonicalHeaderKey(key) {
		case "Accept", "Accept-Encoding", "Accept-Language", "Cache-Control", "Pragma", "Range", "If-Range", "If-Modified-Since", "If-Unmodified-Since", "If-Match", "If-None-Match":
			target[http.CanonicalHeaderKey(key)] = slices.Clone(values)
		}
	}
	if userAgent != "" {
		target.Set("User-Agent", userAgent)
	}
	return target
}

func (c *Client) admittedDo(request *http.Request, priority int, initial initialAdmission) (*http.Response, responseObservation, error) {
	if c.HTTP == nil {
		return nil, responseObservation{}, &httpAdmissionError{cause: fmt.Errorf("HTTP client dependency is required")}
	}
	client := *c.HTTP
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	transport := &admittingTransport{base: base, gate: c.Gate, priority: priority, initial: initial}
	client.Transport = transport
	originalPolicy := client.CheckRedirect
	authorityChanged := false
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return &httpAdmissionError{cause: fmt.Errorf("HTTP redirect request limit reached")}
		}
		if originalPolicy != nil {
			if err := originalPolicy(next, via); err != nil {
				if err == http.ErrUseLastResponse {
					return err
				}
				return &httpAdmissionError{cause: fmt.Errorf("HTTP redirect policy rejected the target")}
			}
		}
		previous := via[len(via)-1].URL
		scheme := strings.ToLower(next.URL.Scheme)
		if (scheme != "https" && scheme != "http") || next.URL.Hostname() == "" {
			return &httpAdmissionError{cause: fmt.Errorf("HTTP redirect target has invalid authority")}
		}
		if strings.EqualFold(previous.Scheme, "https") && scheme != "https" {
			return &httpAdmissionError{cause: fmt.Errorf("HTTP redirect cannot downgrade transport security")}
		}
		changed := !sameAuthority(previous, next.URL)
		if !sameUserInfo(previous, next.URL) || (changed && next.URL.User != nil) {
			return &httpAdmissionError{cause: fmt.Errorf("HTTP redirect cannot introduce credentials")}
		}
		if changed {
			authorityChanged = true
		}
		if authorityChanged {
			if next.Body != nil && next.Body != http.NoBody {
				return &httpAdmissionError{cause: fmt.Errorf("HTTP redirect cannot delegate an opaque request body across authorities")}
			}
			next.Header = safeRedirectHeaders(next.Header, c.UserAgent)
			client.Jar = nil
		}
		return nil
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, responseObservation{}, err
	}
	transport.mu.Lock()
	admitted := transport.lastResponse == response
	observation := transport.lastObservation
	transport.mu.Unlock()
	if !admitted {
		if response.Body != nil {
			err = response.Body.Close()
		}
		return nil, responseObservation{}, errors.Join(fmt.Errorf("HTTP response has no physical admission receipt"), err)
	}
	return response, observation, nil
}

type observedHTTPResponse struct {
	status int
	header http.Header
	data   []byte
	retry  bool
}

type responsePolicy uint8

const (
	providerResponse responsePolicy = iota
	capturedResponse
)

func (c *Client) readAdmitted(request *http.Request, priority int, initial initialAdmission, policy responsePolicy) (result observedHTTPResponse, err error) {
	response, observation, err := c.admittedDo(request, priority, initial)
	if err != nil {
		return observedHTTPResponse{}, err
	}
	defer func() { err = errors.Join(err, response.Body.Close()) }()
	var data []byte
	switch policy {
	case providerResponse:
		data, err = io.ReadAll(response.Body)
	case capturedResponse:
		data, err = io.ReadAll(io.LimitReader(response.Body, FetchLimit+1))
		if err == nil && len(data) > FetchLimit {
			err = ErrTooLarge
		}
	default:
		err = fmt.Errorf("unknown HTTP response policy")
	}
	if err != nil {
		return observedHTTPResponse{}, err
	}
	return observedHTTPResponse{status: response.StatusCode, header: response.Header, data: data, retry: observation.retry}, nil
}

var _ http.RoundTripper = (*admittingTransport)(nil)
