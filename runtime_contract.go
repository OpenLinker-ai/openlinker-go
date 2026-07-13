package openlinker

const (
	RuntimeProtocolVersion = 2
	RuntimeContractID      = "openlinker.runtime.v2"
	RuntimeContractDigest  = "052ed16553eeb896bc7a88dabd1ada77466a4db0c87b55c997c6b91ab72a72de"
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
