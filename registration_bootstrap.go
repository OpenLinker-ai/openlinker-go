package openlinker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func EnsureAgent(ctx context.Context, input any, opts ...RegistrationOption) (*AgentRegistration, error) {
	req, err := resolveEnsureAgentRequest(input, opts...)
	if err != nil {
		return nil, err
	}
	req, stored, err := prepareEnsureAgentRequest(req)
	if err != nil {
		return nil, err
	}
	if stored == nil && req.Policy == RegisterPolicyReuseExisting && req.UserToken == "" && req.AgentToken != "" && req.Name != "" {
		registered, err := RegisterAgentViaToken(ctx, req.APIBase, req.AgentToken, RegisterAgentViaTokenRequest{
			Slug: req.Slug, Name: req.Name, Description: req.Description, EndpointURL: req.EndpointURL,
			EndpointAuthHeader: req.EndpointAuthHeader, PricePerCallCents: req.PricePerCallCents,
			Tags: req.Tags, AbilityTags: req.Tags, SkillIDs: req.SkillIDs, Visibility: req.Visibility,
			ConnectionMode: req.ConnectionMode, MCPToolName: req.MCPToolName,
		})
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		registration := &AgentRegistration{
			AgentID: registered.Agent.ID, AgentSlug: registered.Agent.Slug, AgentName: registered.Agent.Name,
			AgentToken: req.AgentToken, TokenID: registered.AgentToken.ID, TokenPrefix: registered.AgentToken.Prefix,
			APIBase: req.APIBase, RegisteredAt: now, UpdatedAt: now,
		}
		if err := saveAgentRegistration(req.Store, registration); err != nil {
			return nil, err
		}
		return registration, nil
	}
	client, err := NewClient(req.APIBase, WithUserToken(req.UserToken), WithSDKAgent("openlinker-go/register-v2"))
	if err != nil {
		return nil, err
	}
	return client.ensureAgent(ctx, req, stored)
}

func (c *Client) EnsureAgent(ctx context.Context, input any, opts ...RegistrationOption) (*AgentRegistration, error) {
	if c == nil {
		return nil, errors.New("openlinker: client is nil")
	}
	req, err := resolveEnsureAgentRequest(input, opts...)
	if err != nil {
		return nil, err
	}
	req, stored, err := prepareEnsureAgentRequest(req)
	if err != nil {
		return nil, err
	}
	return c.ensureAgent(ctx, req, stored)
}

func (c *Client) ensureAgent(ctx context.Context, req EnsureAgentRequest, stored *AgentRegistration) (*AgentRegistration, error) {
	if req.Policy == RegisterPolicyValidateOnly {
		if stored == nil || req.AgentToken == "" {
			return nil, errors.New("openlinker: no stored Agent registration is available to validate")
		}
		client := c
		if req.UserToken != "" && c.userToken == "" {
			client = c.cloneClient(WithUserToken(req.UserToken))
		}
		if client.userToken == "" {
			return nil, errors.New("openlinker: OPENLINKER_USER_TOKEN is required to validate an Agent registration")
		}
		valid, validateErr := client.validateStoredAgent(ctx, stored)
		if validateErr != nil {
			return nil, validateErr
		}
		if !valid {
			return nil, errors.New("openlinker: no valid stored Agent registration found")
		}
		return effectiveAgentRegistration(stored, req), nil
	}
	if req.Policy != RegisterPolicyRotateToken && req.Policy != RegisterPolicyForceNew && stored != nil && req.AgentToken != "" {
		return effectiveAgentRegistration(stored, req), nil
	}
	client := c
	if req.UserToken != "" && c.userToken == "" {
		client = c.cloneClient(WithUserToken(req.UserToken))
	}
	if client.userToken == "" {
		return nil, errors.New("openlinker: OPENLINKER_USER_TOKEN is required to create an Agent or rotate its Agent Token")
	}
	agent, err := client.ensureCreatorAgent(ctx, req, stored)
	if err != nil {
		return nil, err
	}
	token, err := client.CreateAgentToken(ctx, CreateAgentTokenRequest{
		Name: req.TokenName, AgentID: agent.ID, Scopes: req.TokenScopes, ExpiresInMinutes: req.TokenExpiresInMinutes,
	})
	if err != nil {
		return nil, err
	}
	if token.PlaintextToken == "" {
		return nil, errors.New("openlinker: platform did not return Agent Token plaintext")
	}
	now := time.Now().UTC()
	registeredAt := now
	if stored != nil && !stored.RegisteredAt.IsZero() {
		registeredAt = stored.RegisteredAt
	}
	registration := &AgentRegistration{
		AgentID: agent.ID, AgentSlug: agent.Slug, AgentName: agent.Name,
		AgentToken: token.PlaintextToken, TokenID: token.ID, TokenPrefix: token.Prefix,
		APIBase: req.APIBase, RegisteredAt: registeredAt, UpdatedAt: now,
	}
	if err := saveAgentRegistration(req.Store, registration); err != nil {
		return nil, err
	}
	return registration, nil
}

func prepareEnsureAgentRequest(req EnsureAgentRequest) (EnsureAgentRequest, *AgentRegistration, error) {
	req.UserToken = firstNonEmpty(req.UserToken, os.Getenv("OPENLINKER_USER_TOKEN"))
	req.AgentToken = firstNonEmpty(req.AgentToken, os.Getenv("OPENLINKER_AGENT_TOKEN"))
	req.APIBase = firstNonEmpty(req.APIBase, os.Getenv("OPENLINKER_API_BASE"))
	if req.Store == nil {
		req.Store = NewEnvRegistrationStore(DefaultRegistrationEnvPath)
	}
	stored, err := loadAgentRegistration(req.Store)
	if err != nil {
		return EnsureAgentRequest{}, nil, err
	}
	if stored != nil {
		req.AgentToken = firstNonEmpty(req.AgentToken, stored.AgentToken)
		req.APIBase = firstNonEmpty(req.APIBase, stored.APIBase)
		req.Slug = firstNonEmpty(req.Slug, stored.AgentSlug)
		req.Name = firstNonEmpty(req.Name, stored.AgentName)
	}
	req = normalizeEnsureAgentRequest(req)
	return req, stored, nil
}

func effectiveAgentRegistration(stored *AgentRegistration, req EnsureAgentRequest) *AgentRegistration {
	if stored == nil {
		return nil
	}
	registration := *stored
	registration.AgentToken = firstNonEmpty(req.AgentToken, registration.AgentToken)
	registration.APIBase = firstNonEmpty(req.APIBase, registration.APIBase)
	registration.AgentSlug = firstNonEmpty(req.Slug, registration.AgentSlug)
	registration.AgentName = firstNonEmpty(req.Name, registration.AgentName)
	return &registration
}

func resolveEnsureAgentRequest(input any, opts ...RegistrationOption) (EnsureAgentRequest, error) {
	var req EnsureAgentRequest
	switch value := input.(type) {
	case EnsureAgentRequest:
		req = value
	case *EnsureAgentRequest:
		if value == nil {
			return EnsureAgentRequest{}, errors.New("openlinker: EnsureAgent request is nil")
		}
		req = *value
	case AgentSpec:
		req = ensureAgentRequestFromSpec(value)
	case *AgentSpec:
		if value == nil {
			return EnsureAgentRequest{}, errors.New("openlinker: AgentSpec is nil")
		}
		req = ensureAgentRequestFromSpec(*value)
	default:
		return EnsureAgentRequest{}, errors.New("openlinker: EnsureAgent requires AgentSpec or EnsureAgentRequest")
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	return req, nil
}

func ensureAgentRequestFromSpec(spec AgentSpec) EnsureAgentRequest {
	return EnsureAgentRequest{
		Slug: spec.Slug, Name: spec.Name, Description: spec.Description,
		EndpointURL: spec.EndpointURL, EndpointAuthHeader: spec.EndpointAuthHeader,
		PricePerCallCents: spec.PricePerCallCents, Tags: append([]string(nil), spec.Tags...),
		SkillIDs: append([]string(nil), spec.SkillIDs...), Visibility: spec.Visibility,
		ConnectionMode: spec.ConnectionMode, MCPToolName: spec.MCPToolName,
	}
}

func normalizeEnsureAgentRequest(req EnsureAgentRequest) EnsureAgentRequest {
	req.APIBase = firstNonEmpty(req.APIBase, "https://api.openlinker.ai")
	if req.Policy == "" {
		req.Policy = RegisterPolicyReuseExisting
	}
	if req.Visibility == "" {
		req.Visibility = "private"
	}
	if req.ConnectionMode == "" {
		req.ConnectionMode = "runtime_ws"
	}
	if req.TokenName == "" {
		req.TokenName = firstNonEmpty(req.Name, req.Slug, "Go runtime worker")
	}
	if len(req.TokenScopes) == 0 {
		req.TokenScopes = []string{"agent:pull", "agent:call"}
	}
	if len(req.Tags) == 0 {
		req.Tags = []string{"agent", "runtime"}
	}
	return req
}

func (c *Client) ensureCreatorAgent(ctx context.Context, req EnsureAgentRequest, stored *AgentRegistration) (*AgentResponse, error) {
	if req.Policy != RegisterPolicyForceNew && stored != nil && stored.AgentID != "" {
		agent, err := c.GetMyAgent(ctx, stored.AgentID)
		if err == nil {
			return agent, nil
		}
		if !registrationStatus(err, http.StatusNotFound) {
			return nil, err
		}
	}
	if req.Policy != RegisterPolicyForceNew && req.Slug != "" {
		agent, err := c.GetMyAgentBySlug(ctx, req.Slug)
		if err == nil {
			return agent, nil
		}
		if !registrationStatus(err, http.StatusNotFound) {
			return nil, err
		}
	}
	if req.Slug == "" || req.Name == "" {
		return nil, errors.New("openlinker: Agent slug and name are required to create an Agent")
	}
	return c.CreateAgent(ctx, CreateAgentRequest{
		Slug: req.Slug, Name: req.Name, Description: req.Description, EndpointURL: req.EndpointURL,
		EndpointAuthHeader: req.EndpointAuthHeader, PricePerCallCents: req.PricePerCallCents,
		Tags: req.Tags, SkillIDs: req.SkillIDs, Visibility: req.Visibility,
		ConnectionMode: req.ConnectionMode, MCPToolName: req.MCPToolName,
	})
}

func (c *Client) validateStoredAgent(ctx context.Context, stored *AgentRegistration) (bool, error) {
	if stored == nil || stored.AgentID == "" || stored.TokenID == "" {
		return false, nil
	}
	if _, err := c.GetMyAgent(ctx, stored.AgentID); err != nil {
		return false, err
	}
	tokens, err := c.ListAgentTokens(ctx, ListAgentTokensParams{AgentID: stored.AgentID, Limit: 50})
	if err != nil {
		return false, err
	}
	for _, token := range tokens.Items {
		if token.ID == stored.TokenID && token.Status == "active_runtime" && token.RevokedAt == nil {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) cloneClient(opts ...Option) *Client {
	next := *c
	next.headers = make(http.Header, len(c.headers))
	for key, values := range c.headers {
		next.headers[key] = append([]string(nil), values...)
	}
	for _, opt := range opts {
		opt(&next)
	}
	return &next
}

func loadAgentRegistration(store RegistrationStore) (*AgentRegistration, error) {
	if store == nil {
		return nil, nil
	}
	return store.LoadAgentRegistration()
}

func saveAgentRegistration(store RegistrationStore, registration *AgentRegistration) error {
	if store == nil {
		return nil
	}
	return store.SaveAgentRegistration(registration)
}

func registrationStatus(err error, status int) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.StatusCode == status
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (r AgentRegistration) String() string {
	if r.AgentSlug != "" {
		return fmt.Sprintf("%s (%s)", r.AgentSlug, r.AgentID)
	}
	return strings.TrimSpace(r.AgentID)
}
