package openlinker

type JSON map[string]any

type ListAgentsParams struct {
	Query        string
	Tags         []string
	Page         int
	Size         int
	CallableOnly bool
}

type CreatorMini struct {
	DisplayName string `json:"display_name"`
}

type Availability struct {
	Status              string `json:"status"`
	Label               string `json:"label"`
	Hint                string `json:"hint"`
	LastSuccessfulRunAt string `json:"last_successful_run_at,omitempty"`
	LastFailedRunAt     string `json:"last_failed_run_at,omitempty"`
	LastCheckedAt       string `json:"last_checked_at,omitempty"`
	ConsecutiveFailures int32  `json:"consecutive_failures"`
}

type Readiness struct {
	Listed                 bool              `json:"listed"`
	Discoverable           bool              `json:"discoverable"`
	Callable               bool              `json:"callable"`
	Verified               bool              `json:"verified"`
	Certified              bool              `json:"certified"`
	PaidEnabled            bool              `json:"paid_enabled"`
	AgentCardURL           string            `json:"agent_card_url"`
	A2AEndpoint            string            `json:"a2a_endpoint"`
	LastSuccessfulRunAt    string            `json:"last_successful_run_at,omitempty"`
	AvailabilityStatus     string            `json:"availability_status"`
	VerifiedSkillCount     int32             `json:"verified_skill_count"`
	LatestBenchmarkBatchID string            `json:"latest_benchmark_batch_id,omitempty"`
	Explanation            map[string]string `json:"explanation"`
}

type MarketListItem struct {
	ID                string       `json:"id"`
	Slug              string       `json:"slug"`
	Name              string       `json:"name"`
	Description       string       `json:"description"`
	PricePerCallCents int32        `json:"price_per_call_cents"`
	Tags              []string     `json:"tags"`
	TotalCalls        int32        `json:"total_calls"`
	Creator           CreatorMini  `json:"creator"`
	ConnectionMode    string       `json:"connection_mode"`
	MCPToolName       string       `json:"mcp_tool_name,omitempty"`
	Availability      Availability `json:"availability"`
	Readiness         Readiness    `json:"readiness"`
}

type MarketListResponse struct {
	Items []MarketListItem `json:"items"`
	Total int32            `json:"total"`
	Page  int32            `json:"page"`
	Size  int32            `json:"size"`
}

type SkillMini struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type AgentDetailResponse struct {
	MarketListItem
	EndpointURL         string      `json:"endpoint_url"`
	CreatedAt           string      `json:"created_at"`
	CertifiedAt         string      `json:"certified_at,omitempty"`
	LifecycleStatus     string      `json:"lifecycle_status"`
	Visibility          string      `json:"visibility"`
	CertificationStatus string      `json:"certification_status"`
	VerifiedSkillCount  int32       `json:"verified_skill_count"`
	LatestBenchmarkID   string      `json:"latest_benchmark_batch_id,omitempty"`
	Skills              []SkillMini `json:"skills"`
	Capability          JSON        `json:"capability,omitempty"`
	Examples            []JSON      `json:"examples"`
}

type AgentCardResponse struct {
	Name                              string                `json:"name"`
	Description                       string                `json:"description"`
	URL                               string                `json:"url"`
	Version                           string                `json:"version"`
	ProtocolVersion                   string                `json:"protocolVersion,omitempty"`
	ProtocolVersions                  []string              `json:"protocolVersions,omitempty"`
	PreferredTransport                string                `json:"preferredTransport,omitempty"`
	AdditionalInterfaces              []JSON                `json:"additionalInterfaces,omitempty"`
	SupportedInterfaces               []JSON                `json:"supportedInterfaces,omitempty"`
	SupportsAuthenticatedExtendedCard bool                  `json:"supportsAuthenticatedExtendedCard,omitempty"`
	Provider                          JSON                  `json:"provider"`
	Capabilities                      JSON                  `json:"capabilities"`
	DefaultInputModes                 []string              `json:"default_input_modes"`
	DefaultOutputModes                []string              `json:"default_output_modes"`
	DefaultInputModesCurrent          []string              `json:"defaultInputModes,omitempty"`
	DefaultOutputModesCurrent         []string              `json:"defaultOutputModes,omitempty"`
	Skills                            []JSON                `json:"skills"`
	SecuritySchemes                   JSON                  `json:"securitySchemes,omitempty"`
	Security                          []map[string][]string `json:"security,omitempty"`
	SecurityRequirements              []map[string][]string `json:"securityRequirements,omitempty"`
	Authentication                    JSON                  `json:"authentication"`
	OpenLinker                        JSON                  `json:"openlinker"`
	Capability                        JSON                  `json:"capability,omitempty"`
	Examples                          []JSON                `json:"examples,omitempty"`
	Signature                         JSON                  `json:"signature,omitempty"`
}

type RunAgentRequest struct {
	AgentID  string `json:"agent_id"`
	Input    any    `json:"input"`
	Metadata any    `json:"metadata,omitempty"`
	// IdempotencyKey identifies one run-creation intent across retries. When it
	// is empty, the SDK generates a new key for this method invocation.
	IdempotencyKey         string              `json:"-"`
	A2AContext             *RunA2AContext      `json:"a2a_context,omitempty"`
	TaskCallback           *TaskCallbackConfig `json:"task_callback,omitempty"`
	PushNotification       *TaskCallbackConfig `json:"push_notification,omitempty"`
	PushNotificationConfig *TaskCallbackConfig `json:"pushNotificationConfig,omitempty"`
}

type RunA2AContext struct {
	ProtocolContextID string   `json:"protocol_context_id,omitempty"`
	ProtocolTaskID    string   `json:"protocol_task_id,omitempty"`
	RootContextID     string   `json:"root_context_id,omitempty"`
	ParentContextID   string   `json:"parent_context_id,omitempty"`
	ParentTaskID      string   `json:"parent_task_id,omitempty"`
	ParentRunID       string   `json:"parent_run_id,omitempty"`
	CallerAgentID     string   `json:"caller_agent_id,omitempty"`
	TargetAgentID     string   `json:"target_agent_id,omitempty"`
	TraceID           string   `json:"trace_id,omitempty"`
	ReferenceTaskIDs  []string `json:"reference_task_ids,omitempty"`
	Source            string   `json:"source,omitempty"`
}

type TaskCallbackAuthentication struct {
	Scheme      string `json:"scheme,omitempty"`
	Credentials string `json:"credentials,omitempty"`
}

type TaskCallbackConfig struct {
	URL            string                      `json:"url,omitempty"`
	Token          string                      `json:"token,omitempty"`
	Secret         string                      `json:"secret,omitempty"`
	Authentication *TaskCallbackAuthentication `json:"authentication,omitempty"`
	Metadata       any                         `json:"metadata,omitempty"`
	EventTypes     []string                    `json:"event_types,omitempty"`
}

type TaskCallbackSubscription struct {
	ID                  string   `json:"id"`
	RunID               string   `json:"run_id"`
	TargetURL           string   `json:"target_url"`
	EventTypes          []string `json:"event_types"`
	AuthScheme          string   `json:"auth_scheme,omitempty"`
	Status              string   `json:"status"`
	ConsecutiveFailures int32    `json:"consecutive_failures"`
	Secret              string   `json:"secret,omitempty"`
	CreatedAt           string   `json:"created_at"`
	UpdatedAt           string   `json:"updated_at"`
}

type RunResponse struct {
	RunID                     string                    `json:"run_id"`
	AgentID                   string                    `json:"agent_id,omitempty"`
	AgentSlug                 string                    `json:"agent_slug,omitempty"`
	AgentName                 string                    `json:"agent_name,omitempty"`
	AgentConnectionMode       string                    `json:"agent_connection_mode,omitempty"`
	Status                    string                    `json:"status"`
	Input                     any                       `json:"input,omitempty"`
	Output                    any                       `json:"output,omitempty"`
	ErrorCode                 string                    `json:"error_code,omitempty"`
	ErrorMessage              string                    `json:"error_message,omitempty"`
	CostCents                 int32                     `json:"cost_cents"`
	DurationMS                int32                     `json:"duration_ms"`
	StartedAt                 string                    `json:"started_at"`
	FinishedAt                string                    `json:"finished_at,omitempty"`
	Source                    string                    `json:"source,omitempty"`
	RuntimeContractID         string                    `json:"runtime_contract_id"`
	RuntimeTransport          string                    `json:"runtime_transport,omitempty"`
	RuntimeTransportReason    string                    `json:"runtime_transport_reason,omitempty"`
	RuntimeTransportChangedAt string                    `json:"runtime_transport_changed_at,omitempty"`
	DispatchState             string                    `json:"dispatch_state"`
	AttemptCount              int32                     `json:"attempt_count"`
	MaxAttempts               int32                     `json:"max_attempts"`
	NextAttemptAt             string                    `json:"next_attempt_at,omitempty"`
	LatestAttemptID           string                    `json:"latest_attempt_id,omitempty"`
	ActiveAttemptID           string                    `json:"active_attempt_id,omitempty"`
	CancelState               string                    `json:"cancel_state,omitempty"`
	CancelRequestedAt         string                    `json:"cancel_requested_at,omitempty"`
	CancelAcknowledgedAt      string                    `json:"cancel_acknowledged_at,omitempty"`
	CancelReason              string                    `json:"cancel_reason,omitempty"`
	DeadLetteredAt            string                    `json:"dead_lettered_at,omitempty"`
	ReplayOfRunID             string                    `json:"replay_of_run_id,omitempty"`
	ParentRunID               string                    `json:"parent_run_id,omitempty"`
	CallerAgentID             string                    `json:"caller_agent_id,omitempty"`
	BillingMode               string                    `json:"billing_mode,omitempty"`
	A2AContext                *RunA2AContext            `json:"a2a_context,omitempty"`
	TaskCallback              *TaskCallbackSubscription `json:"task_callback,omitempty"`
	RequirementEvidence       any                       `json:"requirement_evidence,omitempty"`
	EvidenceSummary           any                       `json:"evidence_summary,omitempty"`
	NextAction                any                       `json:"next_action,omitempty"`
	// Replayed is true when Core returned a run created by an earlier request
	// with the same idempotency key and semantic input.
	Replayed bool `json:"replayed"`
}

type ListRunEventsParams struct {
	AfterSequence int32
	Limit         int32
}

type ListRunEventsResponse struct {
	Items []RunEventResponse `json:"items"`
	Meta  RunEventPageMeta   `json:"meta"`
}

// RunEventPageMeta describes the durable event-retention boundary for a page.
// A nil available-sequence bound is encoded by Core as JSON null.
type RunEventPageMeta struct {
	RequestedAfterSequence    int32  `json:"requested_after_sequence"`
	EffectiveAfterSequence    int32  `json:"effective_after_sequence"`
	RetainedThroughSequence   int32  `json:"retained_through_sequence"`
	EarliestAvailableSequence *int32 `json:"earliest_available_sequence"`
	LatestAvailableSequence   *int32 `json:"latest_available_sequence"`
	RetentionGap              bool   `json:"retention_gap"`
	Terminal                  bool   `json:"terminal"`
	StreamComplete            bool   `json:"stream_complete"`
}

type RunChildResponse struct {
	ChildRunID      string             `json:"child_run_id"`
	ParentRunID     string             `json:"parent_run_id"`
	CallerAgentID   string             `json:"caller_agent_id"`
	CallerAgentSlug string             `json:"caller_agent_slug"`
	CallerAgentName string             `json:"caller_agent_name"`
	CallerAgentTags []string           `json:"caller_agent_tags"`
	CallerSkills    []RunSkillRef      `json:"caller_skills"`
	TargetAgentID   string             `json:"target_agent_id"`
	TargetAgentSlug string             `json:"target_agent_slug"`
	TargetAgentName string             `json:"target_agent_name"`
	TargetAgentTags []string           `json:"target_agent_tags"`
	TargetSkills    []RunSkillRef      `json:"target_skills"`
	Reason          string             `json:"reason"`
	Status          string             `json:"status"`
	CostCents       int32              `json:"cost_cents"`
	DurationMS      *int32             `json:"duration_ms,omitempty"`
	StartedAt       string             `json:"started_at"`
	FinishedAt      *string            `json:"finished_at,omitempty"`
	Source          string             `json:"source"`
	BillingMode     string             `json:"billing_mode"`
	A2AContext      *RunA2AContext     `json:"a2a_context,omitempty"`
	Children        []RunChildResponse `json:"children,omitempty"`
}

type RunSkillRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ListRunChildrenResponse struct {
	ParentRunID string             `json:"parent_run_id"`
	Items       []RunChildResponse `json:"items"`
}

type RunEventResponse struct {
	EventID     string `json:"event_id"`
	RunID       string `json:"run_id"`
	ParentRunID string `json:"parent_run_id,omitempty"`
	Sequence    int32  `json:"sequence"`
	EventType   string `json:"event_type"`
	Payload     any    `json:"payload"`
	CreatedAt   string `json:"created_at"`
}

type RunArtifactResponse struct {
	ID               string `json:"id"`
	RunID            string `json:"run_id"`
	ArtifactType     string `json:"artifact_type"`
	Title            string `json:"title"`
	Content          any    `json:"content"`
	Visibility       string `json:"visibility"`
	SourceArtifactID string `json:"source_artifact_id,omitempty"`
	MimeType         string `json:"mime_type,omitempty"`
	FileURI          string `json:"file_uri,omitempty"`
	FileName         string `json:"file_name,omitempty"`
	FileSHA256       string `json:"file_sha256,omitempty"`
	FileSizeBytes    *int64 `json:"file_size_bytes,omitempty"`
	CreatedAt        string `json:"created_at"`
}

type RunMessageResponse struct {
	ID            string `json:"id"`
	RunID         string `json:"run_id"`
	EventSequence *int32 `json:"event_sequence,omitempty"`
	Role          string `json:"role"`
	Content       string `json:"content"`
	Payload       any    `json:"payload"`
	CreatedAt     string `json:"created_at"`
}

type ListItemsResponse[T any] struct {
	Items []T `json:"items"`
}

type StreamRunEventsOptions struct {
	AfterSequence int32
}

type StreamRunEvent struct {
	ID    string
	Event string
	Data  []byte
}

type PlatformCallbackOptions struct {
	EventTypes    []string
	AfterSequence int32
	OnEvent       func(StreamRunEvent) error
	OnTerminal    func(StreamRunEvent) error
	OnClose       func() error
	OnError       func(error)
}
