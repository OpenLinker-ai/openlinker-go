package openlinker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (node *RuntimeWorker) spoolLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var retryTimer *time.Timer
	var retryChannel <-chan time.Time
	retryAttempt := 0
	for {
		select {
		case <-node.runtimeCtx.Done():
			if retryTimer != nil {
				retryTimer.Stop()
			}
			return
		case <-node.wakeSpool:
			if retryChannel != nil {
				continue
			}
		case <-ticker.C:
			if retryChannel != nil {
				continue
			}
		case <-retryChannel:
			retryChannel = nil
		}
		if err := node.flushDurableSpool(); err != nil && node.runtimeCtx.Err() == nil {
			if runtimeErrorIsPermanent(err) || durableRuntimeErrorIsFatal(err) {
				node.reportFatal(scrubRuntimeError(err))
				return
			}
			delay := node.retryDelay(retryAttempt)
			if node.jitter != nil {
				delay = node.jitter(delay)
			} else {
				delay = jitterDuration(delay)
			}
			retryAttempt++
			retryTimer = time.NewTimer(delay)
			retryChannel = retryTimer.C
			node.logf("runtime spool will retry in %s: %v", delay, scrubRuntimeError(err))
			continue
		}
		retryAttempt = 0
	}
}

func (node *RuntimeWorker) flushDurableSpool() error {
	records, err := node.store.Assignments()
	if err != nil {
		return err
	}
	for _, record := range records {
		permission := node.spoolPermission(record.Identity.AttemptID)
		if !permission.events && !permission.result {
			continue
		}
		if err := node.flushAttemptSpool(record, permission); err != nil {
			code := runtimeErrorCode(err)
			if code == "STALE_LEASE" || code == "LEASE_EXPIRED" || code == "RUN_ALREADY_TERMINAL" {
				node.revokeLocalAttempt(record)
				continue
			}
			return err
		}
	}
	return nil
}

func (node *RuntimeWorker) flushAttemptSpool(record AssignmentJournalRecord, permission spoolPermission) error {
	if permission.events {
		events, err := node.store.PendingEvents(record.Identity.AttemptID)
		if err != nil {
			return err
		}
		for _, event := range events {
			var payload map[string]any
			if err := json.Unmarshal(event.Payload, &payload); err != nil || payload == nil {
				return ErrRuntimeRecordCorrupt
			}
			request := RuntimeRunEventPayload{
				AttemptIdentity: sdkAttemptIdentity(event.Identity),
				ClientEventID:   event.ClientEventID,
				ClientEventSeq:  event.ClientEventSeq,
				EventType:       event.EventType,
				Payload:         payload,
			}
			if err := enforceRuntimeMessageLimit(request); err != nil {
				return err
			}
			ack, err := node.runtimeClient.AppendRuntimeEvent(node.runtimeCtx, request)
			if err != nil {
				return err
			}
			if ack == nil || ack.ClientEventID != event.ClientEventID || ack.ClientEventSeq != event.ClientEventSeq {
				return fmt.Errorf("%w: Event ACK identity", ErrRuntimeProtocolMismatch)
			}
			if err := node.store.AckEvent(record.Identity.AttemptID, event.ClientEventID, event.ClientEventSeq); err != nil {
				return err
			}
		}
	}

	if !permission.result {
		return nil
	}
	remainingEvents, err := node.store.PendingEvents(record.Identity.AttemptID)
	if err != nil {
		return err
	}
	if len(remainingEvents) != 0 {
		return nil
	}
	result, err := node.store.PendingResult(record.Identity.AttemptID)
	if errors.Is(err, ErrSpoolRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var request RuntimeRunResultPayload
	if err := decodeStrictJSON(result.Payload, &request); err != nil {
		return ErrRuntimeRecordCorrupt
	}
	if request.AttemptIdentity != sdkAttemptIdentity(result.Identity) || request.ResultID != result.ResultID || request.FinalClientEventSeq != result.FinalClientEventSeq {
		return ErrSpoolRecordConflict
	}
	if err := enforceRuntimeMessageLimit(request); err != nil {
		return err
	}
	var ack *RuntimeRunResultAckPayload
	for repairs := 0; ; repairs++ {
		ack, err = node.runtimeClient.FinalizeRuntimeResult(node.runtimeCtx, request)
		if err == nil {
			break
		}
		ranges, missing := runtimeMissingEventRanges(err)
		if !missing {
			return err
		}
		if repairs >= 1 || len(ranges) == 0 {
			return fmt.Errorf("%w: repeated or empty EVENTS_MISSING response", ErrRuntimeProtocolMismatch)
		}
		if err = node.replayMissingEvents(record, ranges); err != nil {
			return err
		}
	}
	if ack == nil || ack.ResultID != result.ResultID {
		return fmt.Errorf("%w: Result ACK identity", ErrRuntimeProtocolMismatch)
	}
	node.retireActiveAttempt(node.activeAttempt(record.Identity.AttemptID))
	if err := node.store.AckResult(record.Identity.AttemptID, result.ResultID); err != nil {
		return err
	}
	if err := node.store.DeleteAssignment(record.Identity.AssignmentMessageID); err != nil {
		return err
	}
	node.stateMu.Lock()
	delete(node.spoolAllowed, record.Identity.AttemptID)
	node.stateMu.Unlock()
	return nil
}

func (node *RuntimeWorker) replayMissingEvents(record AssignmentJournalRecord, ranges []RuntimeEventRange) error {
	events, err := node.store.EventsInRanges(record.Identity.AttemptID, ranges)
	if err != nil {
		return fmt.Errorf("%w: requested Event range is unavailable: %v", ErrRuntimeProtocolMismatch, err)
	}
	for _, event := range events {
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload == nil {
			return ErrRuntimeRecordCorrupt
		}
		request := RuntimeRunEventPayload{
			AttemptIdentity: sdkAttemptIdentity(event.Identity),
			ClientEventID:   event.ClientEventID,
			ClientEventSeq:  event.ClientEventSeq,
			EventType:       event.EventType,
			Payload:         payload,
		}
		ack, err := node.runtimeClient.AppendRuntimeEvent(node.runtimeCtx, request)
		if err != nil {
			return err
		}
		if ack == nil || ack.ClientEventID != event.ClientEventID || ack.ClientEventSeq != event.ClientEventSeq {
			return fmt.Errorf("%w: missing Event ACK identity", ErrRuntimeProtocolMismatch)
		}
		if err = node.store.AckEvent(record.Identity.AttemptID, event.ClientEventID, event.ClientEventSeq); err != nil {
			return err
		}
	}
	return nil
}

func runtimeMissingEventRanges(err error) ([]RuntimeEventRange, bool) {
	var runtimeErr *Error
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "EVENTS_MISSING" {
		return nil, false
	}
	switch details := runtimeErr.Details.(type) {
	case RuntimeErrorBody:
		return append([]RuntimeEventRange(nil), details.MissingEventRanges...), true
	case *RuntimeErrorBody:
		if details == nil {
			return nil, true
		}
		return append([]RuntimeEventRange(nil), details.MissingEventRanges...), true
	default:
		return nil, true
	}
}

func (node *RuntimeWorker) revokeLocalAttempt(record AssignmentJournalRecord) {
	active := node.activeAttempt(record.Identity.AttemptID)
	if active != nil {
		active.canceled.Store(true)
		active.cancel()
	}
	current, err := node.store.Assignment(record.Identity.AssignmentMessageID)
	if err != nil {
		if !errors.Is(err, ErrAssignmentNotFound) && node.runtimeCtx.Err() == nil {
			node.reportFatal(err)
		}
		node.retireActiveAttempt(active)
		return
	}
	if current.State != AssignmentStateResultACKed && current.State != AssignmentStateRejected && current.State != AssignmentStateRevoked {
		if _, err := node.store.AdvanceAssignment(record.Identity.AssignmentMessageID, AssignmentStateRevoked); err != nil {
			node.reportFatal(err)
		}
	}
	node.stateMu.Lock()
	delete(node.spoolAllowed, record.Identity.AttemptID)
	node.stateMu.Unlock()
	node.retireActiveAttempt(active)
}

func enforceRuntimeMessageLimit(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if int64(len(raw)) > RuntimeMaxMessageBytes {
		return ErrRuntimeMessageTooLarge
	}
	return nil
}
