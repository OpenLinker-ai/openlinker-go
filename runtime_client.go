package openlinker

// Runtime is the Agent-side OpenLinker runtime client. It uses an Agent Token
// for registration/runtime protocol calls and should not be used for user API
// calls such as listing Agents or starting user-initiated runs.
type Runtime struct {
	client *Client
}

func NewRuntime(baseURL string, opts ...Option) (*Runtime, error) {
	client, err := newClient(baseURL, true, opts...)
	if err != nil {
		return nil, err
	}
	return &Runtime{client: client}, nil
}
