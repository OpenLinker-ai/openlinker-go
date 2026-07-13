package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type activeRuntimeAttempt struct {
	identity  AttemptIdentity
	payload   DurableAssignmentPayload
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	renewStop chan struct{}
	renewDone chan struct{}

	canceled atomic.Bool
	finished atomic.Bool

	leaseMu        sync.RWMutex
	leaseExpiresAt time.Time
}

func (attempt *activeRuntimeAttempt) setLeaseExpiry(value time.Time) {
	attempt.leaseMu.Lock()
	attempt.leaseExpiresAt = value
	attempt.leaseMu.Unlock()
}

func (attempt *activeRuntimeAttempt) leaseExpiry() time.Time {
	attempt.leaseMu.RLock()
	defer attempt.leaseMu.RUnlock()
	return attempt.leaseExpiresAt
}

func (node *RuntimeWorker) startConfirmedAttempt(record AssignmentJournalRecord, payload DurableAssignmentPayload, leaseExpiry time.Time) error {
	if record.State != AssignmentStateConfirmed || record.Identity != payload.Identity {
		return ErrAssignmentTransition
	}
	node.stateMu.Lock()
	if node.draining {
		node.stateMu.Unlock()
		return nil
	}
	if existing := node.active[record.Identity.AttemptID]; existing != nil {
		node.stateMu.Unlock()
		return nil
	}
	attemptCtx, cancel := runtimeAttemptContext(node.runtimeCtx, payload)
	attempt := &activeRuntimeAttempt{
		identity:       record.Identity,
		payload:        cloneAssignmentPayload(payload),
		ctx:            attemptCtx,
		cancel:         cancel,
		done:           make(chan struct{}),
		renewStop:      make(chan struct{}),
		renewDone:      make(chan struct{}),
		leaseExpiresAt: leaseExpiry,
	}
	node.active[record.Identity.AttemptID] = attempt
	// shutdown sets draining under the same lock before waiting. Adding here
	// guarantees no execution can be added after shutdown begins its Wait.
	node.executions.Add(1)
	node.stateMu.Unlock()

	if _, err := node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateStarted); err != nil {
		node.removeActiveAttempt(attempt)
		cancel()
		node.executions.Done()
		return err
	}
	go node.executeAttempt(attempt)
	return nil
}

func runtimeAttemptContext(parent context.Context, payload DurableAssignmentPayload) (context.Context, context.CancelFunc) {
	deadline := payload.AttemptDeadlineAt
	if deadline.IsZero() || (!payload.RunDeadlineAt.IsZero() && payload.RunDeadlineAt.Before(deadline)) {
		deadline = payload.RunDeadlineAt
	}
	if deadline.IsZero() {
		return context.WithCancel(parent)
	}
	return context.WithDeadline(parent, deadline)
}

func (node *RuntimeWorker) executeAttempt(attempt *activeRuntimeAttempt) {
	defer node.executions.Done()
	defer close(attempt.done)
	defer node.removeActiveAttempt(attempt)
	go node.renewAttemptLease(attempt)
	defer func() {
		close(attempt.renewStop)
		<-attempt.renewDone
	}()

	input := RuntimeJSONMap{}
	if err := json.Unmarshal(attempt.payload.Input, &input); err != nil {
		node.persistAttemptFailure(attempt, time.Now(), "ASSIGNMENT_INPUT_INVALID", "assignment input could not be decoded")
		return
	}
	metadata := RuntimeJSONMap{}
	if string(attempt.payload.Metadata) != "null" {
		if err := json.Unmarshal(attempt.payload.Metadata, &metadata); err != nil {
			node.persistAttemptFailure(attempt, time.Now(), "ASSIGNMENT_METADATA_INVALID", "assignment metadata could not be decoded")
			return
		}
	}
	startedAt := time.Now()
	if errors.Is(attempt.ctx.Err(), context.DeadlineExceeded) {
		node.persistAttemptFailure(attempt, startedAt, "ATTEMPT_DEADLINE_EXCEEDED", "Attempt deadline elapsed before handler execution")
		return
	}
	handlerCtx, stopHandler := context.WithCancel(attempt.ctx)
	runCtx := RuntimeContext{
		RunID:    attempt.identity.RunID,
		AgentID:  attempt.identity.AgentID,
		Input:    input,
		Metadata: metadata,
	}
	runCtx.emit = func(eventType string, payload any) error {
		if attempt.finished.Load() || attempt.canceled.Load() {
			return context.Canceled
		}
		if err := node.persistRuntimeEvent(attempt, eventType, payload); err != nil {
			if durableRuntimeErrorIsFatal(err) {
				node.reportFatal(err)
			}
			return err
		}
		node.signalSpool()
		return nil
	}
	runCtx.callAgent = func(ctx context.Context, targetAgentID string, input any, options RuntimeCallOptions) (any, error) {
		if attempt.finished.Load() || attempt.canceled.Load() || handlerCtx.Err() != nil {
			return nil, context.Canceled
		}
		if ctx == nil {
			ctx = context.Background()
		}
		callCtx, cancelCall := context.WithCancel(ctx)
		stopCall := context.AfterFunc(handlerCtx, cancelCall)
		defer func() {
			stopCall()
			cancelCall()
		}()
		return node.callAgentForAttempt(callCtx, attempt, targetAgentID, input, options)
	}

	result := RuntimeResult{Status: "success"}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result.Status = "failed"
				result.Error = &RuntimeHandlerError{Code: "HANDLER_PANIC", Message: "runtime handler panicked"}
			}
		}()
		raw, err := node.Handler.Handle(handlerCtx, runCtx)
		if err != nil {
			result.Status = "failed"
			result.Error = normalizeHandlerError(err)
			return
		}
		normalized := normalizeRuntimeResult(raw)
		result.Status = normalized.Status
		result.Output = normalized.Output
		result.Error = normalized.Error
		result.Events = normalized.Events
	}()

	attempt.finished.Store(true)
	stopHandler()
	if attempt.canceled.Load() {
		return
	}
	if errors.Is(attempt.ctx.Err(), context.DeadlineExceeded) {
		result = RuntimeResult{
			Status: "failed",
			Error:  &RuntimeHandlerError{Code: "ATTEMPT_DEADLINE_EXCEEDED", Message: "Attempt deadline elapsed during handler execution"},
		}
	}
	for _, event := range result.Events {
		if err := node.persistRuntimeEvent(attempt, event.EventType, event.Payload); err != nil {
			node.logf("runtime final Event was not persisted: %v", scrubRuntimeError(err))
			if durableRuntimeErrorIsFatal(err) {
				node.reportFatal(err)
				return
			}
			node.persistAttemptFailure(attempt, startedAt, "HANDLER_EVENT_INVALID", "runtime handler returned an invalid final Event")
			return
		}
	}
	result.DurationMS = maxDurationMS(startedAt)
	if err := node.persistRunResult(attempt, result); err != nil {
		node.logf("runtime Result was not persisted: %v", scrubRuntimeError(err))
		node.reportFatal(err)
		return
	}
	node.signalSpool()
}

func (node *RuntimeWorker) persistRuntimeEvent(attempt *activeRuntimeAttempt, eventType string, payload any) error {
	if !runtimeEventTypePattern.MatchString(eventType) || eventType == "run.completed" || eventType == "run.failed" || eventType == "run.canceled" || eventType == "run.stream.gap" {
		return errors.New("invalid or Core-owned runtime Event type")
	}
	payloadMap, err := runtimeObject(payload)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(payloadMap)
	if err != nil {
		return err
	}
	journal, err := node.store.Assignment(attempt.identity.AssignmentMessageID)
	if err != nil {
		return err
	}
	probe := RuntimeRunEventPayload{
		AttemptIdentity: sdkAttemptIdentity(attempt.identity),
		ClientEventID:   "00000000-0000-4000-8000-000000000001",
		ClientEventSeq:  journal.LastClientEventSeq + 1,
		EventType:       eventType,
		Payload:         payloadMap,
	}
	if err := enforceRuntimeMessageLimit(probe); err != nil {
		return err
	}
	_, err = node.store.AppendEvent(attempt.identity, eventType, raw)
	return err
}

func (node *RuntimeWorker) persistRunResult(attempt *activeRuntimeAttempt, result RuntimeResult) error {
	journal, err := node.store.Assignment(attempt.identity.AssignmentMessageID)
	if err != nil {
		return err
	}
	resultID, err := newRuntimeUUID()
	if err != nil {
		return err
	}
	payload, err := runtimeResultPayload(attempt.identity, result)
	if err != nil {
		payload = runtimeFailurePayload(
			attempt.identity,
			"RESULT_INVALID",
			"runtime handler result was not JSON encodable",
			result.DurationMS,
		)
	}
	payload.ResultID = resultID
	payload.FinalClientEventSeq = journal.LastClientEventSeq
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if int64(len(raw)) > RuntimeMaxMessageBytes {
		payload = runtimeFailurePayload(
			attempt.identity,
			"RESULT_TOO_LARGE",
			"runtime handler result exceeded the 4 MiB runtime limit",
			result.DurationMS,
		)
		payload.ResultID = resultID
		payload.FinalClientEventSeq = journal.LastClientEventSeq
		raw, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	return node.store.StoreResult(ResultSpoolRecord{
		Identity:            attempt.identity,
		ResultID:            resultID,
		FinalClientEventSeq: journal.LastClientEventSeq,
		Status:              payload.Status,
		Payload:             raw,
	})
}

func (node *RuntimeWorker) persistAttemptFailure(attempt *activeRuntimeAttempt, startedAt time.Time, code, message string) {
	attempt.finished.Store(true)
	if attempt.canceled.Load() {
		return
	}
	if err := node.persistRunResult(attempt, RuntimeResult{
		Status:     "failed",
		DurationMS: maxDurationMS(startedAt),
		Error:      &RuntimeHandlerError{Code: code, Message: message},
	}); err != nil {
		node.reportFatal(err)
		return
	}
	node.signalSpool()
}

func (node *RuntimeWorker) callAgentForAttempt(
	caller context.Context,
	attempt *activeRuntimeAttempt,
	targetAgentID string,
	input any,
	options RuntimeCallOptions,
) (any, error) {
	if attempt.finished.Load() || attempt.canceled.Load() || attempt.ctx.Err() != nil {
		return nil, context.Canceled
	}
	if caller == nil {
		caller = context.Background()
	}
	inputMap, err := runtimeObject(input)
	if err != nil {
		return nil, err
	}
	metadata, err := runtimeOptionalObject(options.Metadata)
	if err != nil {
		return nil, err
	}
	request := RuntimeCallAgentRequest{
		TargetAgentID: targetAgentID,
		Input:         inputMap,
		Metadata:      metadata,
		Reason:        options.Reason,
	}
	idempotencyKey := options.IdempotencyKey
	if err := validateDelegatedIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	if attempt.finished.Load() || attempt.canceled.Load() || attempt.ctx.Err() != nil {
		return nil, context.Canceled
	}
	callCtx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(attempt.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	summary, err := node.runtimeClient.CallRuntimeAgent(callCtx, RuntimeCallAgentAuthorization{
		NodeEnvelope:         attempt.payload.NodeEnvelope,
		AgentInvocationToken: attempt.payload.AgentInvocationToken,
		IdempotencyKey:       idempotencyKey,
	}, request)
	if err != nil {
		return nil, err
	}
	if summary == nil {
		err := fmt.Errorf("%w: delegated Agent response", ErrRuntimeProtocolMismatch)
		node.reportFatal(err)
		return nil, err
	}
	return RuntimeJSONMap{
		"run_id":         summary.RunID,
		"status":         summary.Status,
		"dispatch_state": summary.DispatchState,
	}, nil
}

func validateDelegatedIdempotencyKey(key string) error {
	if len(key) == 0 {
		return errors.New("idempotency key is required for delegated Agent calls")
	}
	if len(key) > 255 || key[0] == ' ' || key[len(key)-1] == ' ' {
		return errors.New("idempotency key must contain 1 to 255 printable ASCII bytes without surrounding spaces")
	}
	for index := 0; index < len(key); index++ {
		if key[index] < 0x20 || key[index] > 0x7e {
			return errors.New("idempotency key must contain 1 to 255 printable ASCII bytes without surrounding spaces")
		}
	}
	return nil
}

func (node *RuntimeWorker) removeActiveAttempt(attempt *activeRuntimeAttempt) {
	node.stateMu.Lock()
	if node.active[attempt.identity.AttemptID] == attempt {
		delete(node.active, attempt.identity.AttemptID)
	}
	node.stateMu.Unlock()
}

func (node *RuntimeWorker) cancelAllActive() {
	node.stateMu.RLock()
	active := make([]*activeRuntimeAttempt, 0, len(node.active))
	for _, attempt := range node.active {
		active = append(active, attempt)
	}
	node.stateMu.RUnlock()
	for _, attempt := range active {
		attempt.canceled.Store(true)
		attempt.cancel()
	}
}

func (node *RuntimeWorker) activeAttempt(attemptID string) *activeRuntimeAttempt {
	node.stateMu.RLock()
	defer node.stateMu.RUnlock()
	return node.active[attemptID]
}

func runtimeResultPayload(identity AttemptIdentity, result RuntimeResult) (RuntimeRunResultPayload, error) {
	if result.Status != "success" && result.Status != "failed" {
		return runtimeFailurePayload(identity, "RESULT_STATUS_INVALID", "runtime handler returned an invalid result status", result.DurationMS), nil
	}
	if result.Status == "failed" || result.Error != nil {
		agentErr := result.Error
		if agentErr == nil {
			agentErr = &RuntimeHandlerError{Code: "HANDLER_ERROR", Message: "runtime handler returned a failed result"}
		}
		return runtimeFailurePayload(identity, agentErr.Code, agentErr.Message, result.DurationMS), nil
	}
	output, err := runtimeObject(result.Output)
	if err != nil {
		return RuntimeRunResultPayload{}, err
	}
	return RuntimeRunResultPayload{
		AttemptIdentity: sdkAttemptIdentity(identity),
		Status:          "success",
		Output:          output,
		DurationMS:      result.DurationMS,
	}, nil
}

func runtimeFailurePayload(identity AttemptIdentity, code, message string, durationMS int64) RuntimeRunResultPayload {
	return RuntimeRunResultPayload{
		AttemptIdentity: sdkAttemptIdentity(identity),
		Status:          "failed",
		Error: &RuntimeRunErrorPayload{
			ErrorCode:     boundedRuntimeText(code, 120, "HANDLER_ERROR"),
			Message:       boundedRuntimeText(message, 500, "runtime handler failed"),
			RetryableHint: false,
		},
		DurationMS: durationMS,
	}
}

func runtimeObject(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	switch typed := value.(type) {
	case map[string]any:
		return typed, nil
	case RuntimeJSONMap:
		return map[string]any(typed), nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New("runtime value is not JSON encodable")
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err == nil && object != nil {
		return object, nil
	}
	return map[string]any{"value": value}, nil
}

func runtimeOptionalObject(value any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	return runtimeObject(value)
}

func boundedRuntimeText(value string, maximum int, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" || !utf8.ValidString(value) {
		value = fallback
	}
	runes := []rune(value)
	if len(runes) > maximum {
		value = string(runes[:maximum])
	}
	return value
}

func normalizeRuntimeResult(result RuntimeResult) RuntimeResult {
	if result.Status == "" {
		result.Status = "success"
	}
	if result.Output == nil && result.Error == nil {
		result.Output = RuntimeJSONMap{}
	}
	return result
}

func normalizeHandlerError(err error) *RuntimeHandlerError {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return &RuntimeHandlerError{Code: "HANDLER_CANCELED", Message: "runtime handler execution was canceled"}
	}
	return &RuntimeHandlerError{Code: "HANDLER_ERROR", Message: boundedRuntimeText(err.Error(), 500, "runtime handler failed")}
}

func maxDurationMS(startedAt time.Time) int64 {
	milliseconds := time.Since(startedAt).Milliseconds()
	if milliseconds < 1 {
		return 1
	}
	if milliseconds > int64(1<<31-1) {
		return int64(1<<31 - 1)
	}
	return milliseconds
}

func scrubRuntimeError(err error) error {
	if err == nil {
		return nil
	}
	var runtimeErr *Error
	if errors.As(err, &runtimeErr) {
		return fmt.Errorf("%s (HTTP %d)", runtimeErr.Code, runtimeErr.StatusCode)
	}
	return err
}
