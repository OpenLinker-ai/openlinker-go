package openlinker

import "time"

type CreateAgentRequest struct {
	Slug               string   `json:"slug"`
	Name               string   `json:"name"`
	Description        string   `json:"description,omitempty"`
	EndpointURL        string   `json:"endpoint_url,omitempty"`
	EndpointAuthHeader string   `json:"endpoint_auth_header,omitempty"`
	PricePerCallCents  int32    `json:"price_per_call_cents,omitempty"`
	Tags               []string `json:"tags"`
	SkillIDs           []string `json:"skill_ids,omitempty"`
	Visibility         string   `json:"visibility,omitempty"`
	ConnectionMode     string   `json:"connection_mode,omitempty"`
	MCPToolName        string   `json:"mcp_tool_name,omitempty"`
}

type UpdateAgentRequest struct {
	Name               string   `json:"name"`
	Description        string   `json:"description,omitempty"`
	EndpointURL        string   `json:"endpoint_url,omitempty"`
	EndpointAuthHeader string   `json:"endpoint_auth_header,omitempty"`
	ClearEndpointAuth  bool     `json:"clear_endpoint_auth_header,omitempty"`
	PricePerCallCents  int32    `json:"price_per_call_cents,omitempty"`
	Tags               []string `json:"tags"`
	Visibility         string   `json:"visibility,omitempty"`
	ConnectionMode     string   `json:"connection_mode,omitempty"`
	MCPToolName        string   `json:"mcp_tool_name,omitempty"`
}

type ListMyAgentsParams struct {
	Query               string
	Status              string
	Visibility          string
	CertificationStatus string
	SkillIDs            []string
	SortBy              string
	Limit               int32
	Offset              int32
}

type AgentResponse struct {
	ID                  string        `json:"id"`
	Slug                string        `json:"slug"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	EndpointURL         string        `json:"endpoint_url"`
	PricePerCallCents   int32         `json:"price_per_call_cents"`
	Tags                []string      `json:"tags"`
	SkillIDs            []string      `json:"skill_ids,omitempty"`
	Status              string        `json:"status"`
	LifecycleStatus     string        `json:"lifecycle_status"`
	Visibility          string        `json:"visibility"`
	CertificationStatus string        `json:"certification_status"`
	ConnectionMode      string        `json:"connection_mode"`
	MCPToolName         *string       `json:"mcp_tool_name,omitempty"`
	Availability        *Availability `json:"availability,omitempty"`
	Readiness           *Readiness    `json:"readiness,omitempty"`
	CreatedAt           string        `json:"created_at"`
}

type AgentListResponse struct {
	Items  []AgentResponse `json:"items"`
	Total  int32           `json:"total"`
	Limit  int32           `json:"limit"`
	Offset int32           `json:"offset"`
}

type CreateAgentTokenRequest struct {
	Name             string   `json:"name"`
	AgentID          string   `json:"agent_id,omitempty"`
	Scopes           []string `json:"scopes,omitempty"`
	ExpiresInMinutes int32    `json:"expires_in_minutes,omitempty"`
}

type ListAgentTokensParams struct {
	AgentID string
	Limit   int32
	Offset  int32
	SortBy  string
	SortDir string
}

type AgentTokenResponse struct {
	ID             string   `json:"id"`
	AgentID        *string  `json:"agent_id,omitempty"`
	Name           string   `json:"name"`
	Prefix         string   `json:"prefix"`
	Status         string   `json:"status"`
	Scopes         []string `json:"scopes"`
	ExpiresAt      *string  `json:"expires_at,omitempty"`
	RedeemedAt     *string  `json:"redeemed_at,omitempty"`
	RevokedAt      *string  `json:"revoked_at,omitempty"`
	LastUsedAt     *string  `json:"last_used_at,omitempty"`
	CreatedAt      string   `json:"created_at"`
	PlaintextToken string   `json:"plaintext_token,omitempty"`
}

type AgentTokenListResponse struct {
	Items   []AgentTokenResponse `json:"items"`
	Total   int32                `json:"total"`
	Limit   int32                `json:"limit"`
	Offset  int32                `json:"offset"`
	SortBy  string               `json:"sort_by"`
	SortDir string               `json:"sort_dir"`
	HasMore bool                 `json:"has_more"`
}

type RegisterAgentViaTokenRequest struct {
	Slug               string   `json:"slug,omitempty"`
	Name               string   `json:"name"`
	Description        string   `json:"description,omitempty"`
	EndpointURL        string   `json:"endpoint_url,omitempty"`
	EndpointAuthHeader string   `json:"endpoint_auth_header,omitempty"`
	PricePerCallCents  int32    `json:"price_per_call_cents,omitempty"`
	Tags               []string `json:"tags,omitempty"`
	AbilityTags        []string `json:"ability_tags,omitempty"`
	SkillIDs           []string `json:"skill_ids,omitempty"`
	Visibility         string   `json:"visibility,omitempty"`
	ConnectionMode     string   `json:"connection_mode,omitempty"`
	MCPToolName        string   `json:"mcp_tool_name,omitempty"`
}

type RegisterAgentViaTokenResponse struct {
	Agent      AgentResponse      `json:"agent"`
	AgentToken AgentTokenResponse `json:"agent_token"`
}

type RegisterPolicy string

const (
	RegisterPolicyReuseExisting RegisterPolicy = "reuse_existing"
	RegisterPolicyRotateToken   RegisterPolicy = "rotate_token"
	RegisterPolicyForceNew      RegisterPolicy = "force_new"
	RegisterPolicyValidateOnly  RegisterPolicy = "validate_only"
)

// AgentSpec contains the platform-facing description of an Agent. Runtime,
// token policy and persistence settings are supplied separately as options.
type AgentSpec struct {
	Slug               string
	Name               string
	Description        string
	EndpointURL        string
	EndpointAuthHeader string
	PricePerCallCents  int32
	Tags               []string
	SkillIDs           []string
	Visibility         string
	ConnectionMode     string
	MCPToolName        string
}

// RegistrationOption customizes an AgentSpec registration without expanding
// the common Agent description.
type RegistrationOption func(*EnsureAgentRequest)

func WithRegistrationPolicy(value RegisterPolicy) RegistrationOption {
	return func(req *EnsureAgentRequest) { req.Policy = value }
}

func WithRegistrationStore(value RegistrationStore) RegistrationOption {
	return func(req *EnsureAgentRequest) { req.Store = value }
}

func WithRegistrationUserToken(value string) RegistrationOption {
	return func(req *EnsureAgentRequest) { req.UserToken = value }
}

func WithRegistrationAgentToken(value string) RegistrationOption {
	return func(req *EnsureAgentRequest) { req.AgentToken = value }
}

func WithRegistrationAPIBase(value string) RegistrationOption {
	return func(req *EnsureAgentRequest) { req.APIBase = value }
}

func WithRegistrationToken(name string, scopes []string, expiresInMinutes int32) RegistrationOption {
	return func(req *EnsureAgentRequest) {
		req.TokenName = name
		req.TokenScopes = append([]string(nil), scopes...)
		req.TokenExpiresInMinutes = expiresInMinutes
	}
}

// EnsureAgentRequest describes creator-authorized Agent bootstrap. AgentToken
// may contain an already-issued token; UserToken is required when an Agent or
// replacement token must be created.
//
// Deprecated: use AgentSpec with RegistrationOption values for new code.
type EnsureAgentRequest struct {
	Slug               string
	Name               string
	Description        string
	EndpointURL        string
	EndpointAuthHeader string
	PricePerCallCents  int32
	Tags               []string
	SkillIDs           []string
	Visibility         string
	ConnectionMode     string
	MCPToolName        string

	TokenName             string
	TokenScopes           []string
	TokenExpiresInMinutes int32

	Policy     RegisterPolicy
	UserToken  string
	AgentToken string
	APIBase    string
	Store      RegistrationStore
}

type AgentRegistration struct {
	AgentID      string    `json:"agent_id,omitempty"`
	AgentSlug    string    `json:"agent_slug,omitempty"`
	AgentName    string    `json:"agent_name,omitempty"`
	AgentToken   string    `json:"agent_token,omitempty"`
	TokenID      string    `json:"token_id,omitempty"`
	TokenPrefix  string    `json:"token_prefix,omitempty"`
	APIBase      string    `json:"api_base,omitempty"`
	RegisteredAt time.Time `json:"registered_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

type RegistrationStore interface {
	LoadAgentRegistration() (*AgentRegistration, error)
	SaveAgentRegistration(*AgentRegistration) error
}
