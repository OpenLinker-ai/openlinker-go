package openlinker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

const (
	runtimeWorkerDefaultDrainTimeout = 10 * time.Second
	runtimeWorkerMaximumDrainTimeout = 5 * time.Minute
	runtimeWorkerDefaultDrainReason  = "SDK_GRACEFUL_SHUTDOWN"
)

var errRuntimeWorkerStoppedBeforeDrain = errors.New(
	"openlinker: Runtime Worker stopped before its durable drain completed",
)

// Drain first fences local admission, then asks Core to durably drain the
// currently attached Session. It returns success only after Core reports zero
// inflight work and the local handler set plus durable spool are empty.
func (node *RuntimeWorker) Drain(ctx context.Context, options RuntimeWorkerDrainOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}

	node.drainMu.Lock()
	if node.drainDone != nil {
		done := node.drainDone
		node.drainMu.Unlock()
		select {
		case <-done:
			node.drainMu.Lock()
			err := node.drainErr
			node.drainMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	timeout, reasonCode, err := normalizeRuntimeDrainOptions(options)
	if err != nil {
		node.drainMu.Unlock()
		return err
	}
	node.lifecycleMu.Lock()
	started, completed := node.started, node.completed
	node.lifecycleMu.Unlock()
	if !started || completed {
		node.drainMu.Unlock()
		return errors.New("openlinker: Runtime Worker must be running before it can drain")
	}

	// This is the admission linearization point. It happens before the drain
	// operation becomes observable and before any network request.
	node.setDraining(true)
	done := make(chan struct{})
	node.drainDone = done
	node.drainMu.Unlock()

	err = node.performRuntimeDrain(ctx, timeout, reasonCode)
	if err != nil {
		// An unproven drain must fail closed. Shutdown preserves any durable
		// records that did not receive their business ACK.
		node.requestStop()
	}
	node.drainMu.Lock()
	node.drainErr = err
	close(done)
	node.drainMu.Unlock()
	return err
}

func (node *RuntimeWorker) performRuntimeDrain(
	parent context.Context,
	timeout time.Duration,
	reasonCode string,
) error {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	request := RuntimeDrainPayload{
		DeadlineAt: deadline.UTC(),
		ReasonCode: reasonCode,
		Capacity:   0,
	}
	_, request.Inflight = node.capacitySnapshot()
	serverDrain, err := node.requestRuntimeDrain(ctx, request)
	if err != nil {
		return node.runtimeDrainError(err, timeout)
	}

	for {
		status, active, reservations, assignmentOps, statusErr := node.runtimeDrainStatus()
		if statusErr != nil {
			return statusErr
		}
		if status.Empty && active == 0 && reservations == 0 && assignmentOps == 0 {
			if serverDrain.Inflight > 0 {
				serverDrain, err = node.requestRuntimeDrain(ctx, request)
				if err != nil {
					return node.runtimeDrainError(err, timeout)
				}
				if serverDrain.Inflight > 0 {
					if err = node.waitRuntimeDrain(ctx, 25*time.Millisecond); err != nil {
						return node.runtimeDrainError(err, timeout)
					}
					continue
				}
			}
			if node.drainBeforeStop != nil {
				node.drainBeforeStop()
			}
			if !node.requestDrainStop() {
				return errRuntimeWorkerStoppedBeforeDrain
			}
			node.lifecycleMu.Lock()
			done := node.done
			node.lifecycleMu.Unlock()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return node.runtimeDrainError(ctx.Err(), timeout)
			}
		}
		if err = node.waitRuntimeDrain(ctx, 25*time.Millisecond); err != nil {
			return node.runtimeDrainError(err, timeout)
		}
	}
}

func (node *RuntimeWorker) requestRuntimeDrain(
	ctx context.Context,
	request RuntimeDrainPayload,
) (*RuntimeDrainPayload, error) {
	for attempt := 0; ; attempt++ {
		client, runtimeSessionID, attached, stopped := node.runtimeDrainAttachment()
		if stopped {
			return nil, errRuntimeWorkerStoppedBeforeDrain
		}
		if attached {
			drainer, ok := client.(runtimeDrainClient)
			if !ok {
				return nil, fmt.Errorf("%w: Runtime Client does not implement session drain", ErrRuntimeProtocolMismatch)
			}
			callCtx, cancelCall := node.runtimeDrainCallContext(ctx)
			response, err := drainer.DrainRuntimeSession(callCtx, runtimeSessionID, request)
			cancelCall()
			if err == nil {
				if response == nil {
					return nil, fmt.Errorf("%w: empty drain acknowledgement", ErrRuntimeProtocolMismatch)
				}
				if validateErr := validateRuntimeDrain(*response); validateErr != nil {
					return nil, fmt.Errorf("%w: %v", ErrRuntimeProtocolMismatch, validateErr)
				}
				return response, nil
			}
			if runtimeErrorIsPermanent(err) || durableRuntimeErrorIsFatal(err) {
				return nil, err
			}
		}
		delay := node.retryDelay(attempt)
		if delay > 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}
		if err := node.waitRuntimeDrain(ctx, delay); err != nil {
			return nil, err
		}
	}
}

func (node *RuntimeWorker) runtimeDrainAttachment() (
	RuntimeClient,
	string,
	bool,
	bool,
) {
	node.lifecycleMu.Lock()
	stopped := node.stopRequested || node.completed
	node.lifecycleMu.Unlock()
	node.stateMu.RLock()
	ready := node.ready != nil
	client := node.runtimeClient
	store := node.store
	node.stateMu.RUnlock()
	if stopped || !ready || client == nil || store == nil {
		return nil, "", false, stopped
	}
	identity := store.Identity()
	if !runtimeUUID(identity.RuntimeSessionID) {
		return nil, "", false, stopped
	}
	return client, identity.RuntimeSessionID, true, stopped
}

func (node *RuntimeWorker) runtimeDrainStatus() (RuntimeSpoolStatus, int, int, int, error) {
	node.stateMu.RLock()
	store := node.store
	active := len(node.active)
	reservations := len(node.reservations)
	assignmentOps := node.assignmentOps
	node.stateMu.RUnlock()
	if store == nil {
		return RuntimeSpoolStatus{Empty: true}, active, reservations, assignmentOps, nil
	}
	status, err := runtimeStoreSpoolStatus(store)
	return status, active, reservations, assignmentOps, err
}

type runtimeSpoolStatusStore interface {
	SpoolStatus() (RuntimeSpoolStatus, error)
}

func runtimeStoreSpoolStatus(store RuntimeStore) (RuntimeSpoolStatus, error) {
	if provider, ok := store.(runtimeSpoolStatusStore); ok {
		return provider.SpoolStatus()
	}
	records, err := store.Assignments()
	if err != nil {
		return RuntimeSpoolStatus{}, err
	}
	status := RuntimeSpoolStatus{Assignments: len(records)}
	for _, record := range records {
		attemptID := record.Identity.AttemptID
		events, eventErr := store.PendingEvents(attemptID)
		if eventErr != nil {
			return RuntimeSpoolStatus{}, eventErr
		}
		status.Events += len(events)
		if _, resultErr := store.PendingResult(attemptID); resultErr == nil {
			status.Results++
		} else if !errors.Is(resultErr, ErrSpoolRecordNotFound) {
			return RuntimeSpoolStatus{}, resultErr
		}
	}
	status.Empty = status.Assignments == 0 && status.Events == 0 && status.Results == 0
	return status, nil
}

func (node *RuntimeWorker) runtimeDrainError(err error, timeout time.Duration) error {
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	status, _, _, _, statusErr := node.runtimeDrainStatus()
	if statusErr != nil {
		return statusErr
	}
	return &RuntimeDrainTimeoutError{Timeout: timeout, Spool: status}
}

func (node *RuntimeWorker) runtimeDrainCallContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	node.lifecycleMu.Lock()
	stopRequest := node.stopRequest
	node.lifecycleMu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
		case <-stopRequest:
			cancel()
		}
	}()
	return ctx, cancel
}

func (node *RuntimeWorker) waitRuntimeDrain(ctx context.Context, delay time.Duration) error {
	node.lifecycleMu.Lock()
	stopRequest := node.stopRequest
	node.lifecycleMu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stopRequest:
		return errRuntimeWorkerStoppedBeforeDrain
	case <-timer.C:
		return nil
	}
}

func normalizeRuntimeDrainOptions(options RuntimeWorkerDrainOptions) (time.Duration, string, error) {
	timeout := options.Timeout
	if timeout == 0 {
		timeout = runtimeWorkerDefaultDrainTimeout
	}
	if timeout < time.Millisecond || timeout > runtimeWorkerMaximumDrainTimeout {
		return 0, "", errors.New("openlinker: Runtime Worker drain Timeout must be between 1ms and 5m")
	}
	reasonCode := options.ReasonCode
	if reasonCode == "" {
		reasonCode = runtimeWorkerDefaultDrainReason
	}
	if !runtimeText(reasonCode, 120) || strings.TrimSpace(reasonCode) != reasonCode {
		return 0, "", errors.New("openlinker: Runtime Worker drain ReasonCode is invalid")
	}
	for _, char := range reasonCode {
		if unicode.IsControl(char) {
			return 0, "", errors.New("openlinker: Runtime Worker drain ReasonCode is invalid")
		}
	}
	return timeout, reasonCode, nil
}
