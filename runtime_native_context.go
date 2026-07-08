package openlinker

import "context"

type nativeRunContextKey struct{}

func ContextWithNativeRun(ctx context.Context, run NativeRun) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, nativeRunContextKey{}, run)
}

// NativeRunFromContext returns the runtime assignment currently handled by a
// native Agent. It is available inside Agents started with WithAgent or WithFunc.
func NativeRunFromContext(ctx context.Context) (NativeRun, bool) {
	if ctx == nil {
		return NativeRun{}, false
	}
	run, ok := ctx.Value(nativeRunContextKey{}).(NativeRun)
	return run, ok
}
