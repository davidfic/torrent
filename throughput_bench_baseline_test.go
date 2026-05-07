//go:build !benchopt

package torrent

// configureBenchClient is the per-branch hook for opt-in config.
// On master / branches that don't set the benchopt build tag, it's a no-op
// so the benchmark runs against the default config.
func configureBenchClient(cfg *ClientConfig) {}
