package openlinker

import (
	"encoding/json"
	"time"
)

const (
	RuntimeV2MaxMessageBytes    int64 = 4 * 1024 * 1024
	RuntimeV2MaxPullWaitSeconds       = 30
	RuntimeV2MaxNodeCapacity          = 1024
)

type RuntimeV2MessageType string

const (
	RuntimeV2Hello               RuntimeV2MessageType = "runtime.hello"
	RuntimeV2Ready               RuntimeV2MessageType = "runtime.ready"
	RuntimeV2RunAssigned         RuntimeV2MessageType = "run.assigned"
	RuntimeV2AssignmentAck       RuntimeV2MessageType = "run.assignment.ack"
	RuntimeV2AssignmentConfirmed RuntimeV2MessageType = "run.assignment.confirmed"
	RuntimeV2AssignmentReject    RuntimeV2MessageType = "run.assignment.reject"
	RuntimeV2AssignmentRejected  RuntimeV2MessageType = "run.assignment.rejected"
	RuntimeV2LeaseRenew          RuntimeV2MessageType = "run.lease.renew"
	RuntimeV2LeaseRenewed        RuntimeV2MessageType = "run.lease.renewed"
	RuntimeV2RunEvent            RuntimeV2MessageType = "run.event"
	RuntimeV2RunEventAck         RuntimeV2MessageType = "run.event.ack"
	RuntimeV2RunResult           RuntimeV2MessageType = "run.result"
	RuntimeV2RunResultAck        RuntimeV2MessageType = "run.result.ack"
	RuntimeV2RunCancel           RuntimeV2MessageType = "run.cancel"
	RuntimeV2RunCancelAck        RuntimeV2MessageType = "run.cancel.ack"
	RuntimeV2Resume              RuntimeV2MessageType = "runtime.resume"
	RuntimeV2ResumeAccepted      RuntimeV2MessageType = "run.resume.accepted"
	RuntimeV2LeaseRevoked        RuntimeV2MessageType = "run.lease.revoked"
	RuntimeV2Drain               RuntimeV2MessageType = "runtime.drain"
	RuntimeV2Error               RuntimeV2MessageType = "runtime.error"
)

type RuntimeV2EnvelopeFields struct {
	ProtocolVersion   int                  `json:"protocol_version"`
	RuntimeContractID string               `json:"runtime_contract_id"`
	MessageID         string               `json:"message_id"`
	ReplyToMessageID  string               `json:"reply_to_message_id,omitempty"`
	Type              RuntimeV2MessageType `json:"type"`
	SentAt            time.Time            `json:"sent_at"`
}

type RuntimeV2Envelope struct {
	RuntimeV2EnvelopeFields
	Payload json.RawMessage `json:"payload"`
}

type RuntimeV2Message[P any] struct {
	RuntimeV2EnvelopeFields
	Payload P `json:"payload"`
}

type RuntimeV2AttemptIdentity struct {
	RunID            string `json:"run_id"`
	AttemptID        string `json:"attempt_id"`
	LeaseID          string `json:"lease_id"`
	FencingToken     int64  `json:"fencing_token"`
	NodeID           string `json:"node_id"`
	AgentID          string `json:"agent_id"`
	WorkerID         string `json:"worker_id"`
	RuntimeSessionID string `json:"runtime_session_id"`
}

type RuntimeV2HelloPayload struct {
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

type RuntimeV2ReadyPayload struct {
	CoreInstanceID  string    `json:"core_instance_id"`
	Features        []string  `json:"features"`
	OfferTTLSeconds int64     `json:"offer_ttl_seconds"`
	LeaseTTLSeconds int64     `json:"lease_ttl_seconds"`
	DatabaseTime    time.Time `json:"database_time"`
}

type RuntimeV2RunAssignedPayload struct {
	AttemptIdentity      RuntimeV2AttemptIdentity `json:"attempt_identity"`
	OfferNo              int64                    `json:"offer_no"`
	OfferExpiresAt       time.Time                `json:"offer_expires_at"`
	AttemptDeadlineAt    time.Time                `json:"attempt_deadline_at"`
	RunDeadlineAt        time.Time                `json:"run_deadline_at"`
	Input                map[string]any           `json:"input"`
	Metadata             map[string]any           `json:"metadata,omitempty"`
	NodeEnvelope         string                   `json:"node_envelope"`
	AgentInvocationToken string                   `json:"agent_invocation_token"`
}

type RuntimeV2AssignmentAckPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
}

type RuntimeV2AssignmentConfirmedPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	AttemptNo       int64                    `json:"attempt_no"`
	LeaseExpiresAt  time.Time                `json:"lease_expires_at"`
}

type RuntimeV2AssignmentRejectReason string

const (
	RuntimeV2RejectNodeAtCapacity         RuntimeV2AssignmentRejectReason = "NODE_AT_CAPACITY"
	RuntimeV2RejectNodeDraining           RuntimeV2AssignmentRejectReason = "NODE_DRAINING"
	RuntimeV2RejectClientUpgradeRequired  RuntimeV2AssignmentRejectReason = "RUNTIME_CLIENT_UPGRADE_REQUIRED"
	RuntimeV2RejectRequiredFeatureMissing RuntimeV2AssignmentRejectReason = "RUNTIME_REQUIRED_FEATURE_MISSING"
)

type RuntimeV2AssignmentRejectPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity        `json:"attempt_identity"`
	ReasonCode      RuntimeV2AssignmentRejectReason `json:"reason_code"`
	Capacity        int64                           `json:"capacity"`
	Inflight        int64                           `json:"inflight"`
}

type RuntimeV2AssignmentRejectedPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	Outcome         string                   `json:"outcome"`
	DispatchState   string                   `json:"dispatch_state"`
}

type RuntimeV2LeaseRenewPayload struct {
	AttemptIdentity    RuntimeV2AttemptIdentity `json:"attempt_identity"`
	LastClientEventSeq int64                    `json:"last_client_event_seq"`
	Capacity           int64                    `json:"capacity"`
	Inflight           int64                    `json:"inflight"`
}

type RuntimeV2PendingCommand struct {
	Type    RuntimeV2MessageType `json:"type"`
	Payload json.RawMessage      `json:"payload"`
}

type RuntimeV2LeaseRenewedPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	LeaseExpiresAt  time.Time                `json:"lease_expires_at"`
	PendingCommand  *RuntimeV2PendingCommand `json:"pending_command,omitempty"`
}

type RuntimeV2RunEventPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	ClientEventID   string                   `json:"client_event_id"`
	ClientEventSeq  int64                    `json:"client_event_seq"`
	EventType       string                   `json:"event_type"`
	Payload         map[string]any           `json:"payload"`
}

type RuntimeV2RunEventAckPayload struct {
	ClientEventID  string `json:"client_event_id"`
	ClientEventSeq int64  `json:"client_event_seq"`
	Sequence       int64  `json:"sequence"`
	Replayed       bool   `json:"replayed"`
}

type RuntimeV2RunErrorPayload struct {
	ErrorCode     string `json:"error_code"`
	Message       string `json:"message"`
	RetryableHint bool   `json:"retryable_hint"`
}

type RuntimeV2RunResultPayload struct {
	AttemptIdentity     RuntimeV2AttemptIdentity  `json:"attempt_identity"`
	ResultID            string                    `json:"result_id"`
	Status              string                    `json:"status"`
	Output              map[string]any            `json:"output,omitempty"`
	Error               *RuntimeV2RunErrorPayload `json:"error,omitempty"`
	DurationMS          int64                     `json:"duration_ms"`
	FinalClientEventSeq int64                     `json:"final_client_event_seq"`
}

type RuntimeV2RunResultAckPayload struct {
	ResultID       string     `json:"result_id"`
	Classification string     `json:"classification"`
	RunStatus      string     `json:"run_status"`
	DispatchState  string     `json:"dispatch_state"`
	Replayed       bool       `json:"replayed"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
}

type RuntimeV2CancelState string

const (
	RuntimeV2CancelRequested   RuntimeV2CancelState = "requested"
	RuntimeV2CancelDelivered   RuntimeV2CancelState = "delivered"
	RuntimeV2CancelStopping    RuntimeV2CancelState = "stopping"
	RuntimeV2CancelStopped     RuntimeV2CancelState = "stopped"
	RuntimeV2CancelUnsupported RuntimeV2CancelState = "unsupported"
	RuntimeV2CancelFailed      RuntimeV2CancelState = "failed"
	RuntimeV2CancelUnconfirmed RuntimeV2CancelState = "unconfirmed"
)

type RuntimeV2RunCancelPayload struct {
	CancellationID  string                   `json:"cancellation_id"`
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	ReasonCode      string                   `json:"reason_code"`
	DeadlineAt      time.Time                `json:"deadline_at"`
}

type RuntimeV2RunCancelAckPayload struct {
	CancellationID  string                   `json:"cancellation_id"`
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	CancelState     RuntimeV2CancelState     `json:"cancel_state"`
	ErrorCode       string                   `json:"error_code,omitempty"`
}

type RuntimeV2EventRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type RuntimeV2ResumeAttempt struct {
	AttemptIdentity          RuntimeV2AttemptIdentity `json:"attempt_identity"`
	LastAckedClientEventSeq  int64                    `json:"last_acked_client_event_seq"`
	PendingClientEventRanges []RuntimeV2EventRange    `json:"pending_client_event_ranges"`
	PendingResultID          string                   `json:"pending_result_id,omitempty"`
	FinalClientEventSeq      *int64                   `json:"final_client_event_seq,omitempty"`
}

type RuntimeV2ResumePayload struct {
	NodeID           string                   `json:"node_id"`
	AgentID          string                   `json:"agent_id"`
	WorkerID         string                   `json:"worker_id"`
	RuntimeSessionID string                   `json:"runtime_session_id"`
	Attempts         []RuntimeV2ResumeAttempt `json:"attempts"`
}

type RuntimeV2ResumeDecision string

const (
	RuntimeV2ResumeContinue    RuntimeV2ResumeDecision = "continue_execution"
	RuntimeV2ResumeUploadSpool RuntimeV2ResumeDecision = "upload_spool_only"
	RuntimeV2ResumeResultAcked RuntimeV2ResumeDecision = "result_already_acked"
	RuntimeV2ResumeRevoked     RuntimeV2ResumeDecision = "lease_revoked"
)

type RuntimeV2ResumeAcceptedPayload struct {
	AttemptIdentity RuntimeV2AttemptIdentity `json:"attempt_identity"`
	Decision        RuntimeV2ResumeDecision  `json:"decision"`
	LeaseExpiresAt  *time.Time               `json:"lease_expires_at,omitempty"`
	AllowedActions  []string                 `json:"allowed_actions"`
}

type RuntimeV2ResumeResponse struct {
	Decisions []RuntimeV2ResumeAcceptedPayload `json:"decisions"`
}

type RuntimeV2ClaimRequest struct {
	RuntimeSessionID string `json:"runtime_session_id"`
	Capacity         int64  `json:"capacity"`
	Inflight         int64  `json:"inflight"`
}

type RuntimeV2CommandsResponse struct {
	Commands     []RuntimeV2PendingCommand `json:"commands"`
	DatabaseTime time.Time                 `json:"database_time"`
}

type RuntimeV2SessionCloseRequest struct {
	NodeID           string `json:"node_id"`
	AgentID          string `json:"agent_id"`
	WorkerID         string `json:"worker_id"`
	RuntimeSessionID string `json:"runtime_session_id"`
	SessionEpoch     int64  `json:"session_epoch"`
	Status           string `json:"status"`
	Reason           string `json:"reason"`
}

type RuntimeV2ErrorBody struct {
	Code               string                `json:"code"`
	Message            string                `json:"message"`
	Retryable          bool                  `json:"retryable"`
	MissingEventRanges []RuntimeV2EventRange `json:"missing_event_ranges,omitempty"`
}

type RuntimeV2ErrorEnvelope struct {
	Error RuntimeV2ErrorBody `json:"error"`
}
