package openlinker

import (
	"encoding/json"
	"time"
)

const (
	RuntimeMaxMessageBytes    int64 = 4 * 1024 * 1024
	RuntimeMaxPullWaitSeconds       = 30
	RuntimeMaxNodeCapacity          = 1024
)

type RuntimeMessageType string

const (
	RuntimeHello               RuntimeMessageType = "runtime.hello"
	RuntimeReady               RuntimeMessageType = "runtime.ready"
	RuntimeRunAssigned         RuntimeMessageType = "run.assigned"
	RuntimeAssignmentAck       RuntimeMessageType = "run.assignment.ack"
	RuntimeAssignmentConfirmed RuntimeMessageType = "run.assignment.confirmed"
	RuntimeAssignmentReject    RuntimeMessageType = "run.assignment.reject"
	RuntimeAssignmentRejected  RuntimeMessageType = "run.assignment.rejected"
	RuntimeLeaseRenew          RuntimeMessageType = "run.lease.renew"
	RuntimeLeaseRenewed        RuntimeMessageType = "run.lease.renewed"
	RuntimeRunEvent            RuntimeMessageType = "run.event"
	RuntimeRunEventAck         RuntimeMessageType = "run.event.ack"
	RuntimeRunResult           RuntimeMessageType = "run.result"
	RuntimeRunResultAck        RuntimeMessageType = "run.result.ack"
	RuntimeRunCancel           RuntimeMessageType = "run.cancel"
	RuntimeRunCancelAck        RuntimeMessageType = "run.cancel.ack"
	RuntimeResume              RuntimeMessageType = "runtime.resume"
	RuntimeResumeAccepted      RuntimeMessageType = "run.resume.accepted"
	RuntimeLeaseRevoked        RuntimeMessageType = "run.lease.revoked"
	RuntimeDrain               RuntimeMessageType = "runtime.drain"
	RuntimeError               RuntimeMessageType = "runtime.error"
)

type RuntimeEnvelopeFields struct {
	ProtocolVersion   int                `json:"protocol_version"`
	RuntimeContractID string             `json:"runtime_contract_id"`
	MessageID         string             `json:"message_id"`
	ReplyToMessageID  string             `json:"reply_to_message_id,omitempty"`
	Type              RuntimeMessageType `json:"type"`
	SentAt            time.Time          `json:"sent_at"`
}

type RuntimeEnvelope struct {
	RuntimeEnvelopeFields
	Payload json.RawMessage `json:"payload"`
}

type RuntimeMessage[P any] struct {
	RuntimeEnvelopeFields
	Payload P `json:"payload"`
}

type RuntimeAttemptIdentity struct {
	RunID            string `json:"run_id"`
	AttemptID        string `json:"attempt_id"`
	LeaseID          string `json:"lease_id"`
	FencingToken     int64  `json:"fencing_token"`
	NodeID           string `json:"node_id"`
	AgentID          string `json:"agent_id"`
	WorkerID         string `json:"worker_id"`
	RuntimeSessionID string `json:"runtime_session_id"`
}

type RuntimeHelloPayload struct {
	NodeID           string   `json:"node_id"`
	AgentID          string   `json:"agent_id"`
	WorkerID         string   `json:"worker_id"`
	RuntimeSessionID string   `json:"runtime_session_id"`
	SessionEpoch     int64    `json:"session_epoch"`
	NodeVersion      string   `json:"node_version"`
	Capacity         int64    `json:"capacity"`
	Features         []string `json:"features"`
	ContractDigest   string   `json:"contract_digest"`
}

type RuntimeReadyPayload struct {
	CoreInstanceID  string    `json:"core_instance_id"`
	Features        []string  `json:"features"`
	OfferTTLSeconds int64     `json:"offer_ttl_seconds"`
	LeaseTTLSeconds int64     `json:"lease_ttl_seconds"`
	DatabaseTime    time.Time `json:"database_time"`
}

type RuntimeRunAssignedPayload struct {
	AttemptIdentity      RuntimeAttemptIdentity `json:"attempt_identity"`
	OfferNo              int64                  `json:"offer_no"`
	OfferExpiresAt       time.Time              `json:"offer_expires_at"`
	AttemptDeadlineAt    time.Time              `json:"attempt_deadline_at"`
	RunDeadlineAt        time.Time              `json:"run_deadline_at"`
	Input                map[string]any         `json:"input"`
	Metadata             map[string]any         `json:"metadata,omitempty"`
	NodeEnvelope         string                 `json:"node_envelope"`
	AgentInvocationToken string                 `json:"agent_invocation_token"`
}

type RuntimeAssignmentAckPayload struct {
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
}

type RuntimeAssignmentConfirmedPayload struct {
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	AttemptNo       int64                  `json:"attempt_no"`
	LeaseExpiresAt  time.Time              `json:"lease_expires_at"`
}

type RuntimeAssignmentRejectReason string

const (
	RuntimeRejectNodeAtCapacity         RuntimeAssignmentRejectReason = "NODE_AT_CAPACITY"
	RuntimeRejectNodeDraining           RuntimeAssignmentRejectReason = "NODE_DRAINING"
	RuntimeRejectClientUpgradeRequired  RuntimeAssignmentRejectReason = "RUNTIME_CLIENT_UPGRADE_REQUIRED"
	RuntimeRejectRequiredFeatureMissing RuntimeAssignmentRejectReason = "RUNTIME_REQUIRED_FEATURE_MISSING"
)

type RuntimeAssignmentRejectPayload struct {
	AttemptIdentity RuntimeAttemptIdentity        `json:"attempt_identity"`
	ReasonCode      RuntimeAssignmentRejectReason `json:"reason_code"`
	Capacity        int64                         `json:"capacity"`
	Inflight        int64                         `json:"inflight"`
}

type RuntimeAssignmentRejectedPayload struct {
	AttemptIdentity RuntimeAttemptIdentity         `json:"attempt_identity"`
	Outcome         RuntimeAssignmentRejectOutcome `json:"outcome"`
	DispatchState   RuntimeDispatchState           `json:"dispatch_state"`
}

type RuntimeAssignmentRejectOutcome string

const (
	RuntimeOfferRejected          RuntimeAssignmentRejectOutcome = "offer_rejected"
	RuntimeAssignmentLeaseRevoked RuntimeAssignmentRejectOutcome = "lease_revoked"
)

type RuntimeLeaseRenewPayload struct {
	AttemptIdentity    RuntimeAttemptIdentity `json:"attempt_identity"`
	LastClientEventSeq int64                  `json:"last_client_event_seq"`
	Capacity           int64                  `json:"capacity"`
	Inflight           int64                  `json:"inflight"`
}

type RuntimePendingCommand struct {
	Type    RuntimeMessageType `json:"type"`
	Payload json.RawMessage    `json:"payload"`
}

type RuntimeLeaseRenewedPayload struct {
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	LeaseExpiresAt  time.Time              `json:"lease_expires_at"`
	PendingCommand  *RuntimePendingCommand `json:"pending_command,omitempty"`
}

type RuntimeRunEventPayload struct {
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	ClientEventID   string                 `json:"client_event_id"`
	ClientEventSeq  int64                  `json:"client_event_seq"`
	EventType       string                 `json:"event_type"`
	Payload         map[string]any         `json:"payload"`
}

type RuntimeRunEventAckPayload struct {
	ClientEventID  string `json:"client_event_id"`
	ClientEventSeq int64  `json:"client_event_seq"`
	Sequence       int64  `json:"sequence"`
	Replayed       bool   `json:"replayed"`
}

type RuntimeRunErrorPayload struct {
	ErrorCode     string `json:"error_code"`
	Message       string `json:"message"`
	RetryableHint bool   `json:"retryable_hint"`
}

type RuntimeRunResultPayload struct {
	AttemptIdentity     RuntimeAttemptIdentity  `json:"attempt_identity"`
	ResultID            string                  `json:"result_id"`
	Status              string                  `json:"status"`
	Output              map[string]any          `json:"output,omitempty"`
	Error               *RuntimeRunErrorPayload `json:"error,omitempty"`
	DurationMS          int64                   `json:"duration_ms"`
	FinalClientEventSeq int64                   `json:"final_client_event_seq"`
}

type RuntimeRunResultAckPayload struct {
	ResultID       string                      `json:"result_id"`
	Classification RuntimeResultClassification `json:"classification"`
	RunStatus      RuntimeRunStatus            `json:"run_status"`
	DispatchState  RuntimeDispatchState        `json:"dispatch_state"`
	Replayed       bool                        `json:"replayed"`
	NextAttemptAt  *time.Time                  `json:"next_attempt_at,omitempty"`
}

type RuntimeResultClassification string

const (
	RuntimeResultSuccess             RuntimeResultClassification = "success"
	RuntimeResultRetryableFailure    RuntimeResultClassification = "retryable_failure"
	RuntimeResultNonRetryableFailure RuntimeResultClassification = "non_retryable_failure"
	RuntimeResultTimeout             RuntimeResultClassification = "timeout"
	RuntimeResultCanceled            RuntimeResultClassification = "canceled"
	RuntimeResultDeadLetter          RuntimeResultClassification = "dead_letter"
)

type RuntimeRunStatus string

const (
	RuntimeRunRunning  RuntimeRunStatus = "running"
	RuntimeRunSuccess  RuntimeRunStatus = "success"
	RuntimeRunFailed   RuntimeRunStatus = "failed"
	RuntimeRunTimeout  RuntimeRunStatus = "timeout"
	RuntimeRunCanceled RuntimeRunStatus = "canceled"
)

type RuntimeDispatchState string

const (
	RuntimeDispatchPending    RuntimeDispatchState = "pending"
	RuntimeDispatchOffered    RuntimeDispatchState = "offered"
	RuntimeDispatchExecuting  RuntimeDispatchState = "executing"
	RuntimeDispatchRetryWait  RuntimeDispatchState = "retry_wait"
	RuntimeDispatchTerminal   RuntimeDispatchState = "terminal"
	RuntimeDispatchDeadLetter RuntimeDispatchState = "dead_letter"
)

type RuntimeCancelState string

const (
	RuntimeCancelRequested   RuntimeCancelState = "requested"
	RuntimeCancelDelivered   RuntimeCancelState = "delivered"
	RuntimeCancelStopping    RuntimeCancelState = "stopping"
	RuntimeCancelStopped     RuntimeCancelState = "stopped"
	RuntimeCancelUnsupported RuntimeCancelState = "unsupported"
	RuntimeCancelFailed      RuntimeCancelState = "failed"
	RuntimeCancelUnconfirmed RuntimeCancelState = "unconfirmed"
)

type RuntimeRunCancelPayload struct {
	CancellationID  string                 `json:"cancellation_id"`
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	ReasonCode      string                 `json:"reason_code"`
	DeadlineAt      time.Time              `json:"deadline_at"`
}

type RuntimeRunCancelAckPayload struct {
	CancellationID  string                 `json:"cancellation_id"`
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	CancelState     RuntimeCancelState     `json:"cancel_state"`
	ErrorCode       string                 `json:"error_code,omitempty"`
}

type RuntimeRunCancellationState struct {
	CancellationID string             `json:"cancellation_id"`
	CancelState    RuntimeCancelState `json:"cancel_state"`
	UpdatedAt      time.Time          `json:"updated_at"`
	ErrorCode      string             `json:"error_code,omitempty"`
}

type RuntimeEventRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type RuntimeResumeAttempt struct {
	AttemptIdentity          RuntimeAttemptIdentity `json:"attempt_identity"`
	LastAckedClientEventSeq  int64                  `json:"last_acked_client_event_seq"`
	PendingClientEventRanges []RuntimeEventRange    `json:"pending_client_event_ranges"`
	PendingResultID          string                 `json:"pending_result_id,omitempty"`
	FinalClientEventSeq      *int64                 `json:"final_client_event_seq,omitempty"`
}

type RuntimeResumePayload struct {
	NodeID           string                 `json:"node_id"`
	AgentID          string                 `json:"agent_id"`
	WorkerID         string                 `json:"worker_id"`
	RuntimeSessionID string                 `json:"runtime_session_id"`
	Attempts         []RuntimeResumeAttempt `json:"attempts"`
}

type RuntimeResumeDecision string

const (
	RuntimeResumeContinue    RuntimeResumeDecision = "continue_execution"
	RuntimeResumeUploadSpool RuntimeResumeDecision = "upload_spool_only"
	RuntimeResumeResultAcked RuntimeResumeDecision = "result_already_acked"
	RuntimeResumeRevoked     RuntimeResumeDecision = "lease_revoked"
)

type RuntimeResumeAction string

const (
	RuntimeActionContinueExecution RuntimeResumeAction = "continue_execution"
	RuntimeActionUploadEvents      RuntimeResumeAction = "upload_events"
	RuntimeActionUploadResult      RuntimeResumeAction = "upload_result"
	RuntimeActionStopExecution     RuntimeResumeAction = "stop_execution"
	RuntimeActionClearSpool        RuntimeResumeAction = "clear_spool"
)

type RuntimeResumeAcceptedPayload struct {
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	Decision        RuntimeResumeDecision  `json:"decision"`
	LeaseExpiresAt  *time.Time             `json:"lease_expires_at,omitempty"`
	AllowedActions  []RuntimeResumeAction  `json:"allowed_actions"`
}

type RuntimeResumeResponse struct {
	Decisions []RuntimeResumeAcceptedPayload `json:"decisions"`
}

type RuntimeClaimRequest struct {
	RuntimeSessionID string `json:"runtime_session_id"`
	Capacity         int64  `json:"capacity"`
	Inflight         int64  `json:"inflight"`
}

type RuntimeCommandsResponse struct {
	Commands     []RuntimePendingCommand `json:"commands"`
	DatabaseTime time.Time               `json:"database_time"`
}

type RuntimeRunLeaseRevokedPayload struct {
	AttemptIdentity RuntimeAttemptIdentity `json:"attempt_identity"`
	ReasonCode      string                 `json:"reason_code"`
	DispatchState   RuntimeDispatchState   `json:"dispatch_state"`
	RunStatus       RuntimeRunStatus       `json:"run_status"`
}

type RuntimeDrainPayload struct {
	DeadlineAt time.Time `json:"deadline_at"`
	ReasonCode string    `json:"reason_code"`
	Capacity   int64     `json:"capacity"`
	Inflight   int64     `json:"inflight"`
}

type RuntimeDecodedPendingCommand struct {
	Type   RuntimeMessageType
	Cancel *RuntimeRunCancelPayload
	Drain  *RuntimeDrainPayload
	Revoke *RuntimeRunLeaseRevokedPayload
}

type RuntimeCallAgentAuthorization struct {
	NodeEnvelope         string
	AgentInvocationToken string
	IdempotencyKey       string
}

type RuntimeCallAgentRequest struct {
	TargetAgentID string         `json:"target_agent_id"`
	Input         map[string]any `json:"input"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Reason        string         `json:"reason,omitempty"`
}

type RuntimeRunSummary struct {
	RunID         string               `json:"run_id"`
	Status        RuntimeRunStatus     `json:"status"`
	DispatchState RuntimeDispatchState `json:"dispatch_state"`
}

type RuntimeSessionCloseRequest struct {
	NodeID           string `json:"node_id"`
	AgentID          string `json:"agent_id"`
	WorkerID         string `json:"worker_id"`
	RuntimeSessionID string `json:"runtime_session_id"`
	SessionEpoch     int64  `json:"session_epoch"`
	Status           string `json:"status"`
	Reason           string `json:"reason"`
}

type RuntimeErrorBody struct {
	Code                 string               `json:"code"`
	Message              string               `json:"message"`
	Retryable            bool                 `json:"retryable"`
	MissingEventRanges   []RuntimeEventRange  `json:"missing_event_ranges,omitempty"`
	CurrentRunStatus     RuntimeRunStatus     `json:"current_run_status,omitempty"`
	CurrentDispatchState RuntimeDispatchState `json:"current_dispatch_state,omitempty"`
}

type RuntimeErrorEnvelope struct {
	Error RuntimeErrorBody `json:"error"`
}
