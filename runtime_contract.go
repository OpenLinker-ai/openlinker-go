package openlinker

const (
	RuntimeProtocolVersion = 2
	RuntimeContractID      = "openlinker.runtime.v2"
	RuntimeContractDigest  = "857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f"
)

var runtimeRequiredFeatures = [...]string{
	"lease_fence",
	"assignment_confirm",
	"renew",
	"resume",
	"event_ack",
	"result_ack",
	"cancel",
	"persistent_spool",
}

// RuntimeRequiredFeatures returns a copy so callers cannot mutate the
// handshake requirements for the running process.
func RuntimeRequiredFeatures() []string {
	features := make([]string, len(runtimeRequiredFeatures))
	copy(features, runtimeRequiredFeatures[:])
	return features
}
