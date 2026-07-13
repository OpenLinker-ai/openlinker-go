package openlinker

import (
	"errors"
	"fmt"
)

var (
	ErrDataDirLocked             = errors.New("runtime data directory is already in use")
	ErrRuntimeStoreClosed        = errors.New("runtime durable store is closed")
	ErrRuntimeStorePoisoned      = errors.New("runtime durable store requires reopen")
	ErrRuntimeIdentityMissing    = errors.New("runtime identity is missing while durable records exist")
	ErrRuntimeIdentityCorrupt    = errors.New("runtime identity is corrupt")
	ErrRuntimeSpoolKeyMissing    = errors.New("runtime spool key is missing while durable records exist")
	ErrRuntimeSpoolKeyInvalid    = errors.New("runtime spool key is invalid")
	ErrRuntimeRecordCorrupt      = errors.New("runtime durable record is corrupt")
	ErrAssignmentNotFound        = errors.New("assignment journal entry not found")
	ErrAssignmentAlreadyExists   = errors.New("assignment journal entry already exists")
	ErrAttemptAlreadyExists      = errors.New("attempt is already bound to another assignment")
	ErrAssignmentBranchConflict  = errors.New("assignment state branch conflict")
	ErrAssignmentStateRegression = errors.New("assignment state cannot move backward")
	ErrAssignmentTransition      = errors.New("invalid assignment state transition")
	ErrAssignmentCleanup         = errors.New("assignment cannot be removed while durable work remains")
	ErrSpoolRecordConflict       = errors.New("spool record identity conflict")
	ErrSpoolRecordNotFound       = errors.New("spool record not found")
	ErrEventSequence             = errors.New("client event sequence is not monotonic")
	ErrResultAlreadyExists       = errors.New("attempt already has a different durable result")
	ErrRuntimeMessageTooLarge    = errors.New("runtime message exceeds 4 MiB")
	ErrRuntimeProtocolMismatch   = errors.New("runtime protocol response mismatch")
	ErrRuntimeSpoolBackpressure  = errors.New("runtime spool reached the new-Run backpressure threshold")
	ErrRuntimeSpoolFull          = errors.New("runtime spool capacity is exhausted")
)

func durableRuntimeErrorIsFatal(err error) bool {
	for _, target := range []error{
		ErrRuntimeStoreClosed,
		ErrRuntimeStorePoisoned,
		ErrRuntimeRecordCorrupt,
		ErrAssignmentNotFound,
		ErrAssignmentAlreadyExists,
		ErrAttemptAlreadyExists,
		ErrAssignmentBranchConflict,
		ErrAssignmentStateRegression,
		ErrAssignmentTransition,
		ErrAssignmentCleanup,
		ErrSpoolRecordConflict,
		ErrEventSequence,
		ErrResultAlreadyExists,
		ErrRuntimeProtocolMismatch,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// RuntimeRecordError deliberately reports only a record kind and a stable
// reason. Event and result payloads, invocation tokens, and input are never
// included in errors or logs by this package.
type RuntimeRecordError struct {
	Kind   string
	Reason error
}

func (e *RuntimeRecordError) Error() string {
	if e == nil {
		return "runtime durable record error"
	}
	return fmt.Sprintf("runtime %s record: %v", e.Kind, e.Reason)
}

func (e *RuntimeRecordError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Reason
}
