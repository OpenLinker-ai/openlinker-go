package openlinker

const (
	RuntimeProtocolVersion = 2
	RuntimeContractID      = "openlinker.runtime.v2"
	RuntimeContractDigest  = "d83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f"
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
