package openlinker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

type spoolPermission struct {
	events bool
	result bool
}

func (node *RuntimeWorker) runtimeHello() RuntimeHelloPayload {
	identity := node.store.Identity()
	capacity, _ := node.capacitySnapshot()
	return RuntimeHelloPayload{
		NodeID:           node.NodeID,
		AgentID:          node.AgentID,
		WorkerID:         identity.WorkerID,
		RuntimeSessionID: identity.RuntimeSessionID,
		SessionEpoch:     identity.SessionEpoch,
		NodeVersion:      node.NodeVersion,
		Capacity:         capacity,
		Features:         RuntimeRequiredFeatures(),
		ContractDigest:   RuntimeContractDigest,
	}
}

func (node *RuntimeWorker) createSessionWithRetry(parent context.Context) (*RuntimeReadyPayload, error) {
	return node.createSessionWithRetryClient(parent, node.runtimeClient)
}

func (node *RuntimeWorker) createSessionWithRetryClient(parent context.Context, client RuntimeClient) (*RuntimeReadyPayload, error) {
	for attempt := 0; ; attempt++ {
		if err := firstContextError(parent, node.runtimeCtx); err != nil {
			return nil, err
		}
		callCtx, cancel := context.WithTimeout(parent, 20*time.Second)
		ready, err := client.CreateRuntimeSession(callCtx, node.runtimeHello())
		cancel()
		if err == nil {
			if ready == nil {
				return nil, fmt.Errorf("%w: session ready response", ErrRuntimeProtocolMismatch)
			}
			return ready, nil
		}
		if (runtimeErrorIsPermanent(err) && !runtimeAttachErrorIsRetryable(err)) || durableRuntimeErrorIsFatal(err) {
			return nil, scrubRuntimeError(err)
		}
		if err := node.waitRetry(parent, attempt); err != nil {
			return nil, err
		}
	}
}

func (node *RuntimeWorker) heartbeatLoop() {
	for {
		timer := time.NewTimer(node.runtimeHeartbeatInterval())
		select {
		case <-node.runtimeCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := node.heartbeatOnce(node.runtimeCtx); err != nil {
				if runtimeErrorIsPermanent(err) || durableRuntimeErrorIsFatal(err) {
					node.reportFatal(scrubRuntimeError(err))
					return
				}
				node.logf("runtime heartbeat retrying: %v", scrubRuntimeError(err))
			}
		}
	}
}

func (node *RuntimeWorker) heartbeatOnce(ctx context.Context) error {
	if node.store == nil || node.runtimeClient == nil {
		return nil
	}
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ready, err := node.runtimeClient.HeartbeatRuntimeSession(callCtx, node.runtimeHello())
	if err != nil {
		return err
	}
	if ready == nil {
		return fmt.Errorf("%w: heartbeat ready response", ErrRuntimeProtocolMismatch)
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	return nil
}

func (node *RuntimeWorker) claimLoop() {
	attempt := 0
	for {
		if node.runtimeCtx.Err() != nil {
			return
		}
		capacity, inflight := node.capacitySnapshot()
		if capacity == 0 || inflight >= capacity {
			if sleepContext(node.runtimeCtx, 100*time.Millisecond) != nil {
				return
			}
			continue
		}
		assigned, err := node.runtimeClient.ClaimRuntimeRun(node.runtimeCtx, durationSeconds(node.ClaimWait), RuntimeClaimRequest{
			RuntimeSessionID: node.store.Identity().RuntimeSessionID,
			Capacity:         capacity,
			Inflight:         inflight,
		})
		if err != nil {
			if runtimeErrorIsPermanent(err) {
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
		if assigned == nil {
			continue
		}
		if err := node.handleClaimedAssignment(assigned); err != nil {
			if runtimeErrorIsPermanent(err) || durableRuntimeErrorIsFatal(err) {
				node.reportFatal(scrubRuntimeError(err))
				return
			}
			node.logf("runtime assignment deferred: %v", scrubRuntimeError(err))
		}
	}
}

func (node *RuntimeWorker) handleClaimedAssignment(assigned *RuntimeRunAssignedPayload) error {
	localIdentity, err := node.localAttemptIdentity(assigned.AttemptIdentity)
	if err != nil {
		return err
	}
	input, err := json.Marshal(assigned.Input)
	if err != nil {
		return errors.New("assignment input is not JSON encodable")
	}
	metadata, err := json.Marshal(assigned.Metadata)
	if err != nil {
		return errors.New("assignment metadata is not JSON encodable")
	}
	inputDigest := sha256.Sum256(input)
	contextDigest := sha256.Sum256([]byte(assigned.NodeEnvelope))
	journal := AssignmentJournalRecord{
		Identity:            localIdentity,
		InputDigest:         hex.EncodeToString(inputDigest[:]),
		SignedContextDigest: hex.EncodeToString(contextDigest[:]),
	}
	if err := node.store.CreateAssignment(journal); err != nil {
		return err
	}
	record, err := node.store.Assignment(localIdentity.AssignmentMessageID)
	if err != nil {
		return err
	}
	capacity, inflight := node.capacitySnapshot()
	if record.State == AssignmentStateReceived && (capacity == 0 || inflight >= capacity) {
		return node.rejectAssignment(record)
	}
	payload := DurableAssignmentPayload{
		Identity:             localIdentity,
		Input:                input,
		Metadata:             metadata,
		NodeEnvelope:         assigned.NodeEnvelope,
		AgentInvocationToken: assigned.AgentInvocationToken,
		OfferExpiresAt:       assigned.OfferExpiresAt,
		AttemptDeadlineAt:    assigned.AttemptDeadlineAt,
		RunDeadlineAt:        assigned.RunDeadlineAt,
	}
	if err := node.store.StoreAssignmentPayload(payload); err != nil {
		return err
	}

	if record.State == AssignmentStateReceived {
		if _, err := node.store.AdvanceAssignment(localIdentity.AssignmentMessageID, AssignmentStateACKSent); err != nil {
			return err
		}
		record, _ = node.store.Assignment(localIdentity.AssignmentMessageID)
	}
	if record.State == AssignmentStateACKSent {
		confirmed, err := node.ackAssignmentWithRetry(record.Identity)
		if err != nil {
			if runtimeErrorCode(err) == "STALE_LEASE" || runtimeErrorCode(err) == "LEASE_EXPIRED" {
				_, _ = node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRevoked)
			}
			return err
		}
		if confirmed == nil || confirmed.AttemptIdentity != sdkAttemptIdentity(record.Identity) || !confirmed.LeaseExpiresAt.After(time.Now()) {
			return fmt.Errorf("%w: assignment confirmation", ErrRuntimeProtocolMismatch)
		}
		if _, err := node.store.AdvanceAssignment(localIdentity.AssignmentMessageID, AssignmentStateConfirmed); err != nil {
			return err
		}
		node.allowSpool(localIdentity.AttemptID, spoolPermission{events: true, result: true})
		record, _ = node.store.Assignment(localIdentity.AssignmentMessageID)
		return node.startConfirmedAttempt(record, payload, confirmed.LeaseExpiresAt)
	}
	if record.State == AssignmentStateConfirmed {
		node.allowSpool(localIdentity.AttemptID, spoolPermission{events: true, result: true})
		return node.startConfirmedAttempt(record, payload, time.Time{})
	}
	if record.State == AssignmentStateStarted || record.State == AssignmentStateFinished {
		// A transport replacement can replay the exact durable offer. Re-ACK it
		// so the new socket/pull request is not left outstanding, but never call
		// the handler again for an Attempt that crossed the started boundary.
		confirmed, err := node.ackAssignmentWithRetry(record.Identity)
		if err != nil {
			return err
		}
		if confirmed == nil || confirmed.AttemptIdentity != sdkAttemptIdentity(record.Identity) {
			return fmt.Errorf("%w: duplicate assignment confirmation", ErrRuntimeProtocolMismatch)
		}
		node.allowSpool(localIdentity.AttemptID, spoolPermission{events: true, result: true})
		if active := node.activeAttempt(localIdentity.AttemptID); active != nil {
			active.setLeaseExpiry(confirmed.LeaseExpiresAt)
		}
		return nil
	}
	return nil
}

func (node *RuntimeWorker) ackAssignmentWithRetry(identity AttemptIdentity) (*RuntimeAssignmentConfirmedPayload, error) {
	for attempt := 0; ; attempt++ {
		confirmed, err := node.runtimeClient.AckRuntimeAssignment(node.runtimeCtx, RuntimeAssignmentAckPayload{
			AttemptIdentity: sdkAttemptIdentity(identity),
		})
		if err == nil {
			return confirmed, nil
		}
		if runtimeErrorIsPermanent(err) || runtimeErrorCode(err) == "STALE_LEASE" || runtimeErrorCode(err) == "LEASE_EXPIRED" {
			return nil, err
		}
		if node.waitRetry(node.runtimeCtx, attempt) != nil {
			return nil, node.runtimeCtx.Err()
		}
	}
}

func (node *RuntimeWorker) rejectAssignment(record AssignmentJournalRecord) error {
	if record.State == AssignmentStateReceived {
		if _, err := node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRejectSent); err != nil {
			return err
		}
	}
	capacity, inflight := node.capacitySnapshot()
	reason := RuntimeRejectNodeAtCapacity
	if node.isDraining() {
		reason = RuntimeRejectNodeDraining
	}
	for attempt := 0; ; attempt++ {
		_, err := node.runtimeClient.RejectRuntimeAssignment(node.runtimeCtx, RuntimeAssignmentRejectPayload{
			AttemptIdentity: sdkAttemptIdentity(record.Identity),
			ReasonCode:      reason,
			Capacity:        capacity,
			Inflight:        inflight,
		})
		if err == nil {
			_, _ = node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRejected)
			return node.store.DeleteAssignment(record.Identity.AssignmentMessageID)
		}
		if runtimeErrorIsPermanent(err) || runtimeErrorCode(err) == "STALE_LEASE" || runtimeErrorCode(err) == "LEASE_EXPIRED" {
			_, _ = node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRevoked)
			return err
		}
		if node.waitRetry(node.runtimeCtx, attempt) != nil {
			return node.runtimeCtx.Err()
		}
	}
}

func (node *RuntimeWorker) resumeDurableState(parent context.Context) error {
	return node.resumeDurableStateWithClient(parent, node.runtimeClient, false)
}

func (node *RuntimeWorker) resumeDurableStateWithClient(parent context.Context, client RuntimeClient, reconnect bool) error {
	records, err := node.store.Assignments()
	if err != nil || len(records) == 0 {
		return err
	}
	sort.Slice(records, func(left, right int) bool {
		return records[left].Identity.AttemptID < records[right].Identity.AttemptID
	})
	request := RuntimeResumePayload{
		NodeID:           node.NodeID,
		AgentID:          node.AgentID,
		WorkerID:         node.store.Identity().WorkerID,
		RuntimeSessionID: node.store.Identity().RuntimeSessionID,
		Attempts:         make([]RuntimeResumeAttempt, 0, len(records)),
	}
	for _, record := range records {
		pendingEvents, err := node.store.PendingEvents(record.Identity.AttemptID)
		if err != nil {
			return err
		}
		resumeAttempt := RuntimeResumeAttempt{
			AttemptIdentity:          sdkAttemptIdentity(record.Identity),
			LastAckedClientEventSeq:  record.AckedClientEventSeq,
			PendingClientEventRanges: eventRanges(pendingEvents),
		}
		if result, resultErr := node.store.PendingResult(record.Identity.AttemptID); resultErr == nil {
			resumeAttempt.PendingResultID = result.ResultID
			finalSequence := result.FinalClientEventSeq
			resumeAttempt.FinalClientEventSeq = &finalSequence
		} else if !errors.Is(resultErr, ErrSpoolRecordNotFound) {
			return resultErr
		}
		request.Attempts = append(request.Attempts, resumeAttempt)
	}

	var response *RuntimeResumeResponse
	for attempt := 0; ; attempt++ {
		callCtx, cancel := context.WithTimeout(parent, 20*time.Second)
		response, err = client.ResumeRuntimeRuns(callCtx, request)
		cancel()
		if err == nil {
			break
		}
		if runtimeErrorIsPermanent(err) {
			return scrubRuntimeError(err)
		}
		if err := node.waitRetry(parent, attempt); err != nil {
			return err
		}
	}
	if response == nil || len(response.Decisions) != len(records) {
		return fmt.Errorf("%w: resume response count", ErrRuntimeProtocolMismatch)
	}
	for index, decision := range response.Decisions {
		record := records[index]
		if decision.AttemptIdentity != sdkAttemptIdentity(record.Identity) {
			return fmt.Errorf("%w: resume response identity", ErrRuntimeProtocolMismatch)
		}
		switch decision.Decision {
		case RuntimeResumeContinue:
			if reconnect && record.State == AssignmentStateStarted {
				active := node.activeAttempt(record.Identity.AttemptID)
				if active == nil {
					return errors.New("unsafe reconnect refused: started Attempt has no live handler")
				}
				node.allowSpool(record.Identity.AttemptID, spoolPermission{events: true, result: true})
				if decision.LeaseExpiresAt != nil {
					active.setLeaseExpiry(*decision.LeaseExpiresAt)
				}
				continue
			}
			if reconnect && record.State == AssignmentStateFinished {
				node.allowSpool(record.Identity.AttemptID, spoolPermission{events: true, result: true})
				if active := node.activeAttempt(record.Identity.AttemptID); active != nil && decision.LeaseExpiresAt != nil {
					active.setLeaseExpiry(*decision.LeaseExpiresAt)
				}
				continue
			}
			if record.State == AssignmentStateStarted || record.State == AssignmentStateFinished {
				return errors.New("unsafe resume refused: a previous process had already started this Attempt")
			}
			if record.State == AssignmentStateACKSent {
				if _, err := node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateConfirmed); err != nil {
					return err
				}
				record, _ = node.store.Assignment(record.Identity.AssignmentMessageID)
			}
			if record.State != AssignmentStateConfirmed {
				return ErrAssignmentTransition
			}
			payload, err := node.store.AssignmentPayload(record.Identity.AttemptID)
			if err != nil {
				return errors.New("confirmed assignment payload is unavailable")
			}
			node.allowSpool(record.Identity.AttemptID, spoolPermission{events: true, result: true})
			leaseExpiry := time.Time{}
			if decision.LeaseExpiresAt != nil {
				leaseExpiry = *decision.LeaseExpiresAt
			}
			if err := node.startConfirmedAttempt(record, payload, leaseExpiry); err != nil {
				return err
			}
		case RuntimeResumeUploadSpool:
			if reconnect {
				if err := node.stopActiveAttemptForResume(parent, record.Identity.AttemptID); err != nil {
					return err
				}
			}
			permission := spoolPermission{}
			for _, action := range decision.AllowedActions {
				permission.events = permission.events || action == RuntimeActionUploadEvents
				permission.result = permission.result || action == RuntimeActionUploadResult
			}
			node.allowSpool(record.Identity.AttemptID, permission)
		case RuntimeResumeResultAcked, RuntimeResumeRevoked:
			if reconnect {
				if err := node.stopActiveAttemptForResume(parent, record.Identity.AttemptID); err != nil {
					return err
				}
			}
			if err := node.clearAttemptFromResume(record, decision.Decision); err != nil {
				return err
			}
		}
	}
	node.signalSpool()
	return nil
}

func (node *RuntimeWorker) stopActiveAttemptForResume(ctx context.Context, attemptID string) error {
	active := node.activeAttempt(attemptID)
	if active == nil {
		return nil
	}
	active.canceled.Store(true)
	active.cancel()
	select {
	case <-active.done:
		node.retireActiveAttempt(active)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (node *RuntimeWorker) clearAttemptFromResume(record AssignmentJournalRecord, decision RuntimeResumeDecision) error {
	if decision == RuntimeResumeRevoked && record.State != AssignmentStateResultACKed && record.State != AssignmentStateRejected && record.State != AssignmentStateRevoked {
		if _, err := node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRevoked); err != nil {
			return err
		}
		record, _ = node.store.Assignment(record.Identity.AssignmentMessageID)
	}
	events, err := node.store.PendingEvents(record.Identity.AttemptID)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := node.store.AckEvent(record.Identity.AttemptID, event.ClientEventID, event.ClientEventSeq); err != nil {
			return err
		}
	}
	if result, resultErr := node.store.PendingResult(record.Identity.AttemptID); resultErr == nil {
		if err := node.store.AckResult(record.Identity.AttemptID, result.ResultID); err != nil {
			return err
		}
		record, _ = node.store.Assignment(record.Identity.AssignmentMessageID)
	} else if !errors.Is(resultErr, ErrSpoolRecordNotFound) {
		return resultErr
	}
	if record.State == AssignmentStateRevoked || record.State == AssignmentStateResultACKed {
		if err := node.store.ClearTerminalEvents(record.Identity.AttemptID); err != nil {
			return err
		}
	}
	return node.store.DeleteAssignment(record.Identity.AssignmentMessageID)
}

func (node *RuntimeWorker) localAttemptIdentity(identity RuntimeAttemptIdentity) (AttemptIdentity, error) {
	current := node.store.Identity()
	if identity.NodeID != node.NodeID || identity.AgentID != node.AgentID || identity.WorkerID != current.WorkerID || identity.RuntimeSessionID != current.RuntimeSessionID {
		return AttemptIdentity{}, ErrSpoolRecordConflict
	}
	return AttemptIdentity{
		NodeID:              identity.NodeID,
		AgentID:             identity.AgentID,
		WorkerID:            identity.WorkerID,
		RuntimeSessionID:    identity.RuntimeSessionID,
		SessionEpoch:        current.SessionEpoch,
		AssignmentMessageID: deterministicRuntimeUUID("assignment", identity.AttemptID, identity.LeaseID),
		RunID:               identity.RunID,
		AttemptID:           identity.AttemptID,
		OfferID:             deterministicRuntimeUUID("offer", identity.AttemptID, identity.LeaseID),
		LeaseID:             identity.LeaseID,
		FencingToken:        identity.FencingToken,
	}, nil
}

func sdkAttemptIdentity(identity AttemptIdentity) RuntimeAttemptIdentity {
	return RuntimeAttemptIdentity{
		RunID:            identity.RunID,
		AttemptID:        identity.AttemptID,
		LeaseID:          identity.LeaseID,
		FencingToken:     identity.FencingToken,
		NodeID:           identity.NodeID,
		AgentID:          identity.AgentID,
		WorkerID:         identity.WorkerID,
		RuntimeSessionID: identity.RuntimeSessionID,
	}
}

func deterministicRuntimeUUID(parts ...string) string {
	digest := sha256.Sum256([]byte(joinRuntimeIdentityParts(parts)))
	digest[6] = (digest[6] & 0x0f) | 0x50
	digest[8] = (digest[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
}

func joinRuntimeIdentityParts(parts []string) string {
	// IDs belong to the SDK Runtime contract, not to any temporary Adapter.
	// This pre-1.0 cutover intentionally does not preserve the Agent Node domain.
	value := "openlinker/runtime/deterministic-id"
	for _, part := range parts {
		value += "\x00" + part
	}
	return value
}

func eventRanges(events []EventSpoolRecord) []RuntimeEventRange {
	if len(events) == 0 {
		return []RuntimeEventRange{}
	}
	ranges := make([]RuntimeEventRange, 0, len(events))
	start := events[0].ClientEventSeq
	end := start
	for _, event := range events[1:] {
		if event.ClientEventSeq == end+1 {
			end = event.ClientEventSeq
			continue
		}
		ranges = append(ranges, RuntimeEventRange{Start: start, End: end})
		start, end = event.ClientEventSeq, event.ClientEventSeq
	}
	return append(ranges, RuntimeEventRange{Start: start, End: end})
}

func (node *RuntimeWorker) allowSpool(attemptID string, permission spoolPermission) {
	node.stateMu.Lock()
	node.spoolAllowed[attemptID] = permission
	node.stateMu.Unlock()
}

func (node *RuntimeWorker) spoolPermission(attemptID string) spoolPermission {
	node.stateMu.RLock()
	defer node.stateMu.RUnlock()
	return node.spoolAllowed[attemptID]
}

func (node *RuntimeWorker) waitRetry(ctx context.Context, attempt int) error {
	delay := node.retryDelay(attempt)
	if node.jitter != nil {
		delay = node.jitter(delay)
	} else {
		delay = jitterDuration(delay)
	}
	return sleepContext(ctx, delay)
}

func (node *RuntimeWorker) retryDelay(attempt int) time.Duration {
	minimum, maximum := node.runtimeRetryPolicy()
	delay := minimum
	for index := 0; index < attempt && delay < maximum; index++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func jitterDuration(value time.Duration) time.Duration {
	if value <= 0 {
		return value
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return value
	}
	// Uniform factor in [0.8, 1.2].
	fraction := float64(binary.BigEndian.Uint64(random[:])) / float64(^uint64(0))
	return time.Duration(float64(value) * (0.8 + 0.4*fraction))
}

func durationSeconds(value time.Duration) int {
	seconds := int(value / time.Second)
	if seconds < 0 {
		return 0
	}
	if seconds > RuntimeMaxPullWaitSeconds {
		return RuntimeMaxPullWaitSeconds
	}
	return seconds
}

func firstContextError(contexts ...context.Context) error {
	for _, ctx := range contexts {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func runtimeErrorIsPermanent(err error) bool {
	var recoveryErr *runtimePolicyRecoveryError
	if errors.As(err, &recoveryErr) || runtimePolicyRecoverySignal(err) {
		return true
	}
	code := runtimeErrorCode(err)
	switch code {
	case "UNAUTHORIZED", "FORBIDDEN", "PERMISSION_DENIED", "RUNTIME_CLIENT_UPGRADE_REQUIRED", "RUNTIME_REQUIRED_FEATURE_MISSING", "RUNTIME_SESSION_CONFLICT":
		return true
	}
	var runtimeErr *Error
	return errors.As(err, &runtimeErr) && runtimeErr.StatusCode >= 400 && runtimeErr.StatusCode < 500 && runtimeErr.StatusCode != 408 && runtimeErr.StatusCode != 409 && runtimeErr.StatusCode != 429
}

func runtimeAttachErrorIsRetryable(err error) bool {
	return runtimeErrorCode(err) == "RUNTIME_SESSION_CONFLICT"
}

func runtimeErrorCode(err error) string {
	var runtimeErr *Error
	if errors.As(err, &runtimeErr) {
		return runtimeErr.Code
	}
	return ""
}

func validRuntimeUUID(value string) bool {
	if len(value) != 36 || value == "00000000-0000-0000-0000-000000000000" || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
