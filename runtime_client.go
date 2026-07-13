package openlinker

import "sync"

// Runtime is the Agent-side OpenLinker runtime client. It uses an Agent Token
// for registration/runtime protocol calls and should not be used for user API
// calls such as listing Agents or starting user-initiated runs.
type Runtime struct {
	client *Client
	// attachmentMu is both the lifecycle gate and the attachment state lock.
	// Create, heartbeat, and close take it exclusively so an older response can
	// never overwrite a newer generation. All other Pull calls hold a read lock
	// through the request so a reattach cannot rotate their generation midway.
	attachmentMu sync.RWMutex
	attachmentID string
}

func NewRuntime(baseURL string, opts ...Option) (*Runtime, error) {
	client, err := newClient(baseURL, true, opts...)
	if err != nil {
		return nil, err
	}
	return &Runtime{client: client}, nil
}
