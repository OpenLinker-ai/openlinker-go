package openlinker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (node *RuntimeWorker) commandLoop() {
	attempt := 0
	for {
		if node.runtimeCtx.Err() != nil {
			return
		}
		response, err := node.runtimeClient.PollRuntimeCommands(
			node.runtimeCtx,
			node.store.Identity().RuntimeSessionID,
			durationSeconds(node.CommandWait),
		)
		if err != nil {
			if runtimeErrorIsPermanent(err) || durableRuntimeErrorIsFatal(err) {
				node.reportFatal(scrubRuntimeError(err))
				return
			}
			if node.waitRetry(node.runtimeCtx, attempt) != nil {
				return
			}
			attempt++
			continue
		}
		attempt = 0
		if response == nil {
			node.reportFatal(fmt.Errorf("%w: command poll response", ErrRuntimeProtocolMismatch))
			return
		}
		for _, command := range response.Commands {
			decoded, err := command.Decode()
			if err != nil {
				node.reportFatal(err)
				return
			}
			node.handleDecodedCommand(decoded)
		}
	}
}

func (node *RuntimeWorker) handleDecodedCommand(command RuntimeDecodedPendingCommand) {
	switch command.Type {
	case RuntimeRunCancel:
		if command.Cancel == nil || !node.beginCancellation(command.Cancel.CancellationID) {
			return
		}
		node.loops.Add(1)
		go func() {
			defer node.loops.Done()
			defer node.finishCancellation(command.Cancel.CancellationID)
			node.handleCancelCommand(*command.Cancel)
		}()
	case RuntimeDrain:
		node.setDraining(true)
	case RuntimeLeaseRevoked:
		if command.Revoke != nil {
			node.handleLeaseRevoke(*command.Revoke)
		}
	}
}

func (node *RuntimeWorker) finishCancellation(cancellationID string) {
	node.stateMu.Lock()
	delete(node.cancellations, cancellationID)
	node.stateMu.Unlock()
}

func (node *RuntimeWorker) beginCancellation(cancellationID string) bool {
	node.stateMu.Lock()
	defer node.stateMu.Unlock()
	if _, exists := node.cancellations[cancellationID]; exists {
		return false
	}
	node.cancellations[cancellationID] = struct{}{}
	return true
}

func (node *RuntimeWorker) handleCancelCommand(command RuntimeRunCancelPayload) {
	record, err := node.assignmentByAttempt(command.AttemptIdentity.AttemptID)
	if err != nil || sdkAttemptIdentity(record.Identity) != command.AttemptIdentity {
		_ = node.ackCancelUntil(command, RuntimeCancelFailed, "ATTEMPT_IDENTITY_MISMATCH")
		return
	}
	if err := node.ackCancelOnce(command, RuntimeCancelStopping, ""); err != nil {
		node.logf("runtime cancel stopping ACK was not confirmed: %v", scrubRuntimeError(err))
	}
	active := node.activeAttempt(record.Identity.AttemptID)
	if active != nil {
		active.canceled.Store(true)
		active.cancel()
		waitUntil := command.DeadlineAt
		if waitUntil.IsZero() {
			waitUntil = time.Now().Add(RuntimeWorkerDefaultShutdownTimeout)
		}
		timer := time.NewTimer(time.Until(waitUntil))
		select {
		case <-active.done:
			timer.Stop()
		case <-timer.C:
			_ = node.ackCancelUntil(command, RuntimeCancelFailed, "CANCEL_DEADLINE_EXCEEDED")
			return
		case <-node.runtimeCtx.Done():
			timer.Stop()
			return
		}
	}
	if err := node.ackCancelUntil(command, RuntimeCancelStopped, ""); err != nil {
		node.logf("runtime cancel stopped ACK will retry: %v", scrubRuntimeError(err))
		return
	}
	node.stateMu.Lock()
	delete(node.spoolAllowed, record.Identity.AttemptID)
	node.stateMu.Unlock()
	current, err := node.store.Assignment(record.Identity.AssignmentMessageID)
	if err != nil {
		if !errors.Is(err, ErrAssignmentNotFound) && node.runtimeCtx.Err() == nil {
			node.reportFatal(err)
		}
		return
	}
	if current.State != AssignmentStateResultACKed && current.State != AssignmentStateRejected && current.State != AssignmentStateRevoked {
		if _, err := node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRevoked); err != nil {
			node.reportFatal(err)
		}
	}
	node.retireActiveAttempt(node.activeAttempt(record.Identity.AttemptID))
}

func (node *RuntimeWorker) ackCancelOnce(command RuntimeRunCancelPayload, state RuntimeCancelState, errorCode string) error {
	ctx, cancel := context.WithTimeout(node.runtimeCtx, 2*time.Second)
	defer cancel()
	_, err := node.runtimeClient.AckRuntimeCancel(ctx, RuntimeRunCancelAckPayload{
		CancellationID:  command.CancellationID,
		AttemptIdentity: command.AttemptIdentity,
		CancelState:     state,
		ErrorCode:       errorCode,
	})
	return err
}

func (node *RuntimeWorker) ackCancelUntil(command RuntimeRunCancelPayload, state RuntimeCancelState, errorCode string) error {
	deadline := command.DeadlineAt
	for attempt := 0; ; attempt++ {
		ctx := node.runtimeCtx
		var cancel context.CancelFunc = func() {}
		if !deadline.IsZero() {
			ctx, cancel = context.WithDeadline(node.runtimeCtx, deadline)
		}
		_, err := node.runtimeClient.AckRuntimeCancel(ctx, RuntimeRunCancelAckPayload{
			CancellationID:  command.CancellationID,
			AttemptIdentity: command.AttemptIdentity,
			CancelState:     state,
			ErrorCode:       errorCode,
		})
		cancel()
		if err == nil {
			return nil
		}
		if runtimeErrorIsPermanent(err) || (!deadline.IsZero() && !time.Now().Before(deadline)) {
			return err
		}
		if node.waitRetry(node.runtimeCtx, attempt) != nil {
			return node.runtimeCtx.Err()
		}
	}
}

func (node *RuntimeWorker) handleLeaseRevoke(command RuntimeRunLeaseRevokedPayload) {
	record, err := node.assignmentByAttempt(command.AttemptIdentity.AttemptID)
	if err != nil || sdkAttemptIdentity(record.Identity) != command.AttemptIdentity {
		return
	}
	node.revokeLocalAttempt(record)
}

func (node *RuntimeWorker) renewAttemptLease(attempt *activeRuntimeAttempt) {
	defer close(attempt.renewDone)
	interval := node.leaseRenewInterval()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	retry := 0
	for {
		select {
		case <-attempt.renewCtx.Done():
			return
		case <-attempt.renewStop:
			return
		case <-timer.C:
		}
		if expiry := attempt.leaseExpiry(); !expiry.IsZero() && !time.Now().Before(expiry) {
			attempt.canceled.Store(true)
			attempt.cancel()
			if record, err := node.store.Assignment(attempt.identity.AssignmentMessageID); err == nil {
				node.revokeLocalAttempt(record)
			}
			return
		}
		record, err := node.store.Assignment(attempt.identity.AssignmentMessageID)
		if err != nil {
			if node.runtimeCtx.Err() != nil || (attempt.finished.Load() && errors.Is(err, ErrAssignmentNotFound)) {
				return
			}
			node.reportFatal(err)
			return
		}
		capacity, inflight := node.capacitySnapshot()
		renewCtx := attempt.renewCtx
		var cancelRenew context.CancelFunc
		if expiry := attempt.leaseExpiry(); !expiry.IsZero() {
			renewCtx, cancelRenew = context.WithDeadline(attempt.renewCtx, expiry)
		} else {
			renewCtx, cancelRenew = context.WithTimeout(attempt.renewCtx, 10*time.Second)
		}
		renewed, err := node.runtimeClient.RenewRuntimeLease(renewCtx, RuntimeLeaseRenewPayload{
			AttemptIdentity:    sdkAttemptIdentity(attempt.identity),
			LastClientEventSeq: record.LastClientEventSeq,
			Capacity:           capacity,
			Inflight:           inflight,
		})
		cancelRenew()
		if err != nil {
			code := runtimeErrorCode(err)
			if code == "STALE_LEASE" || code == "LEASE_EXPIRED" || code == "RUN_CANCEL_REQUESTED" {
				attempt.canceled.Store(true)
				attempt.cancel()
				node.revokeLocalAttempt(record)
				return
			}
			if runtimeErrorIsPermanent(err) {
				node.reportFatal(scrubRuntimeError(err))
				attempt.canceled.Store(true)
				attempt.cancel()
				return
			}
			if expiry := attempt.leaseExpiry(); !expiry.IsZero() && !time.Now().Before(expiry) {
				attempt.canceled.Store(true)
				attempt.cancel()
				node.revokeLocalAttempt(record)
				return
			}
			delay := node.retryDelay(retry)
			if node.jitter != nil {
				delay = node.jitter(delay)
			} else {
				delay = jitterDuration(delay)
			}
			retry++
			if expiry := attempt.leaseExpiry(); !expiry.IsZero() && delay > time.Until(expiry) {
				delay = max(time.Until(expiry), time.Millisecond)
			}
			timer.Reset(delay)
			continue
		}
		retry = 0
		if renewed == nil || renewed.AttemptIdentity != sdkAttemptIdentity(attempt.identity) || !renewed.LeaseExpiresAt.After(time.Now()) {
			node.reportFatal(fmt.Errorf("%w: lease renewal", ErrRuntimeProtocolMismatch))
			attempt.canceled.Store(true)
			attempt.cancel()
			return
		}
		attempt.setLeaseExpiry(renewed.LeaseExpiresAt)
		if renewed.PendingCommand != nil {
			decoded, decodeErr := renewed.PendingCommand.Decode()
			if decodeErr != nil {
				node.reportFatal(decodeErr)
				return
			}
			node.handleDecodedCommand(decoded)
		}
		timer.Reset(interval)
	}
}

func (node *RuntimeWorker) leaseRenewInterval() time.Duration {
	node.stateMu.RLock()
	ready := node.ready
	node.stateMu.RUnlock()
	if ready != nil && ready.LeaseTTLSeconds > 0 {
		interval := time.Duration(ready.LeaseTTLSeconds) * time.Second / 3
		if interval < 250*time.Millisecond {
			interval = 250 * time.Millisecond
		}
		return interval
	}
	return RuntimeWorkerDefaultHeartbeatInterval
}

func (node *RuntimeWorker) assignmentByAttempt(attemptID string) (AssignmentJournalRecord, error) {
	records, err := node.store.Assignments()
	if err != nil {
		return AssignmentJournalRecord{}, err
	}
	for _, record := range records {
		if record.Identity.AttemptID == attemptID {
			return record, nil
		}
	}
	return AssignmentJournalRecord{}, ErrAssignmentNotFound
}
