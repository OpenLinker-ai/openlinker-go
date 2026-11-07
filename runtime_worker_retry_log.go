package openlinker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// runtimeRetryLogInterval bounds how often one retry cause is logged for one
	// operation. The first occurrence is always logged.
	runtimeRetryLogInterval = 30 * time.Second
	// runtimeRetryLogMaxCauses bounds limiter memory when causes vary.
	runtimeRetryLogMaxCauses = 64
	// runtimePlatformCodeMaxLength bounds a platform error or reason code.
	runtimePlatformCodeMaxLength = 64
	// runtimeUnrecognizedCode replaces any code outside the platform shape.
	runtimeUnrecognizedCode = "UNRECOGNIZED_CODE"
)

// runtimeRetryLogLimiter rate-limits diagnostic logs for automatic retries.
// It only decides whether to log; it never changes retry timing or errors.
// The zero value is ready to use.
type runtimeRetryLogLimiter struct {
	mu      sync.Mutex
	entries map[string]*runtimeRetryLogEntry
	// overflow rate-limits every new cause that arrives while all tracked
	// causes are still inside their interval, so memory stays bounded without
	// forgetting (and re-logging) the causes that are tracked.
	overflow runtimeRetryLogEntry
}

type runtimeRetryLogEntry struct {
	last       time.Time
	suppressed int
}

// allow reports whether a line for key may be logged at now. When it may, it
// also returns how many occurrences were suppressed since the previous line.
func (limiter *runtimeRetryLogLimiter) allow(key string, now time.Time, interval time.Duration) (bool, int) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if limiter.entries == nil {
		limiter.entries = make(map[string]*runtimeRetryLogEntry)
	}
	entry := limiter.entries[key]
	if entry == nil {
		if len(limiter.entries) >= runtimeRetryLogMaxCauses && !limiter.evictOldestExpiredLocked(now, interval) {
			return allowRuntimeRetryLogEntry(&limiter.overflow, now, interval)
		}
		if !limiter.overflow.last.IsZero() && now.Sub(limiter.overflow.last) < interval {
			// The cause may already have logged through the overflow bucket.
			// Promotion keeps that cooldown so no cause logs twice per interval.
			limiter.entries[key] = &runtimeRetryLogEntry{last: limiter.overflow.last, suppressed: 1}
			return false, 0
		}
		limiter.entries[key] = &runtimeRetryLogEntry{last: now}
		return true, 0
	}
	return allowRuntimeRetryLogEntry(entry, now, interval)
}

// evictOldestExpiredLocked frees one slot by removing the least recently
// logged cause whose interval has passed. Unexpired causes are never dropped.
func (limiter *runtimeRetryLogLimiter) evictOldestExpiredLocked(now time.Time, interval time.Duration) bool {
	oldestKey := ""
	var oldest time.Time
	for key, entry := range limiter.entries {
		if now.Sub(entry.last) < interval {
			continue
		}
		if oldestKey == "" || entry.last.Before(oldest) {
			oldestKey, oldest = key, entry.last
		}
	}
	if oldestKey == "" {
		return false
	}
	delete(limiter.entries, oldestKey)
	return true
}

func allowRuntimeRetryLogEntry(entry *runtimeRetryLogEntry, now time.Time, interval time.Duration) (bool, int) {
	if !entry.last.IsZero() && now.Sub(entry.last) < interval {
		entry.suppressed++
		return false, 0
	}
	suppressed := entry.suppressed
	entry.last = now
	entry.suppressed = 0
	return true, suppressed
}

// logRuntimeRetry records why an automatic retry happens without changing the
// retry or the error. The text is built only from safe parts, never from error
// message text; repeated causes are rate-limited.
func (node *RuntimeWorker) logRuntimeRetry(operation string, err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrRuntimeTransportSwitching) {
		// Cancellation and transport switching are consequences of a shutdown or
		// transition that is logged where it starts.
		return
	}
	message := runtimeRetryLogText(err)
	allowed, suppressed := node.retryLogs.allow(operation+"\x00"+runtimeRetryCause(err, message), time.Now(), runtimeRetryLogInterval)
	if !allowed {
		return
	}
	if suppressed > 0 {
		node.logf("runtime %s retrying: %s (%d similar retries not logged)", operation, message, suppressed)
		return
	}
	node.logf("runtime %s retrying: %s", operation, message)
}

// runtimeRetryLogText describes err for a log line using only safe parts: the
// platform code and HTTP status, a WebSocket close code, a fixed
// network/TLS/timeout class, a known SDK condition, or the Go type names of
// the error chain. It never includes error message text, which can carry
// credentials, and it only reads err.
func runtimeRetryLogText(err error) string {
	var runtimeErr *Error
	if errors.As(err, &runtimeErr) {
		return fmt.Sprintf("%s (HTTP %d)", runtimeLoggableCode(runtimeErr.Code), runtimeErr.StatusCode)
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		// The close reason is peer-controlled text and is never logged.
		return "WebSocket closed with code " + strconv.Itoa(closeErr.Code)
	}
	var capacityErr *runtimeWebSocketCapacityChangedError
	if errors.As(err, &capacityErr) {
		return fmt.Sprintf("WebSocket attached with capacity %d; current capacity %d applies on the next attach",
			capacityErr.Attached, capacityErr.Current)
	}
	if class := runtimeRetryErrorClass(err); class != "" {
		return class
	}
	return "unclassified error (" + runtimeErrorTypeChain(err) + ")"
}

// runtimeErrorTypeChain names the Go types along err's Unwrap chain, for
// example "*fmt.wrapError > *errors.errorString". Type names carry no message
// text.
func runtimeErrorTypeChain(err error) string {
	names := make([]string, 0, 4)
	for current := err; current != nil && len(names) < 4; current = errors.Unwrap(current) {
		names = append(names, runtimeErrorTypeName(current))
	}
	return strings.Join(names, " > ")
}

// runtimeErrorTypeName returns a named type as its package-qualified name, for
// example "*url.Error". Anonymous, reflection-built or otherwise unusual types
// print their structure (fields and tags), so they are reported as "unnamed".
func runtimeErrorTypeName(err error) string {
	typ := reflect.TypeOf(err)
	base := typ
	for base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Name() == "" {
		return "unnamed"
	}
	name := typ.String()
	for index := 0; index < len(name); index++ {
		character := name[index]
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9', character == '_', character == '.', character == '*':
		default:
			// Generic instantiations and similar can embed other type text.
			return "unnamed"
		}
	}
	return name
}

// runtimeRetryErrorClass maps transport failures to a fixed description.
func runtimeRetryErrorClass(err error) string {
	operation := ""
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		switch urlErr.Op {
		case "Get", "Post", "Put", "Patch", "Delete", "Head":
			operation = urlErr.Op + " request: "
		}
	}
	var dnsErr *net.DNSError
	var certificateErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidCertificate x509.CertificateInvalidError
	var recordHeaderErr tls.RecordHeaderError
	var alertErr tls.AlertError
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return operation + "timeout"
	case errors.Is(err, context.Canceled):
		return operation + "canceled"
	case errors.Is(err, syscall.ECONNREFUSED):
		return operation + "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return operation + "connection reset"
	case errors.Is(err, syscall.EPIPE):
		return operation + "broken pipe"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return operation + "connection closed unexpectedly"
	case errors.As(err, &dnsErr):
		return operation + "DNS lookup failed"
	case errors.As(err, &certificateErr), errors.As(err, &unknownAuthority), errors.As(err, &hostnameErr),
		errors.As(err, &invalidCertificate), errors.As(err, &recordHeaderErr), errors.As(err, &alertErr):
		return operation + "TLS handshake failed"
	case errors.As(err, &netErr) && netErr.Timeout():
		return operation + "timeout"
	case errors.Is(err, ErrRuntimeProtocolMismatch):
		return "Runtime protocol mismatch"
	case errors.As(err, &netErr), urlErr != nil:
		return operation + "network error"
	}
	return ""
}

// runtimeRetryCause groups retries by their stable cause. Digits are folded so
// ephemeral ports or counters cannot defeat the rate limit.
func runtimeRetryCause(err error, message string) string {
	var runtimeErr *Error
	if errors.As(err, &runtimeErr) {
		// Only a validated code is kept, even in memory.
		return "status:" + strconv.Itoa(runtimeErr.StatusCode) + ":" + runtimeLoggableCode(runtimeErr.Code)
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return "ws_close:" + strconv.Itoa(closeErr.Code)
	}
	var folded strings.Builder
	previousDigit := false
	for _, character := range message {
		if character >= '0' && character <= '9' {
			if !previousDigit {
				folded.WriteByte('#')
			}
			previousDigit = true
			continue
		}
		previousDigit = false
		folded.WriteRune(character)
	}
	return "message:" + folded.String()
}

// runtimePlatformErrorCode returns the platform error code from a bounded Core
// error body ({"error":{"code":...}}): the code itself when it has the
// platform code shape, runtimeUnrecognizedCode for any other code, and "" when
// the body has no code. It never returns response text.
func runtimePlatformErrorCode(body []byte) string {
	if len(body) == 0 || len(body) > openLinkerDiscoveryMaxBytes {
		return ""
	}
	var parsed errorResponse
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.Error.Code == "" {
		return ""
	}
	return runtimeLoggableCode(parsed.Error.Code)
}

// runtimePlatformCodeShape reports whether value has the platform's error and
// reason code shape ^[A-Z][A-Z0-9_]{1,63}$, for example NODE_AT_CAPACITY or
// SDK_GRACEFUL_SHUTDOWN. Platform credentials (ol_..._ prefixes) never match.
func runtimePlatformCodeShape(value string) bool {
	if len(value) < 2 || len(value) > runtimePlatformCodeMaxLength {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character >= 'A' && character <= 'Z':
		case index > 0 && (character >= '0' && character <= '9' || character == '_'):
		default:
			return false
		}
	}
	return true
}

// runtimeLoggableCode returns value when it has the platform code shape and a
// fixed fallback otherwise, so a code field can never carry other text into a
// log line, an error message or a limiter key.
func runtimeLoggableCode(value string) string {
	if runtimePlatformCodeShape(value) {
		return value
	}
	return runtimeUnrecognizedCode
}
