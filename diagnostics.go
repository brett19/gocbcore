package gocbcore

import (
	"sync"
	"sync/atomic"
	"time"
)

// PingState is the current state of a endpoint used in a PingResult.
type PingState uint8

const (
	// PingStateOK indicates that an endpoint is OK.
	PingStateOK PingState = iota

	// PingStateTimeout indicates that the ping request to an endpoint timed out.
	PingStateTimeout PingState = iota

	// PingStateError indicates that the ping request to an endpoint encountered an error.
	PingStateError PingState = iota
)

// EndpointPingResult contains the results of a ping to a single server.
type EndpointPingResult struct {
	Endpoint string
	Error    error
	Latency  time.Duration
	ID       string
	Scope    string
	State    PingState
}

type pingSubOp struct {
	op       PendingOp
	endpoint string
}

type pingOp struct {
	lock       sync.Mutex
	subops     []pingSubOp
	remaining  int32
	results    map[ServiceType][]EndpointPingResult
	callback   PingCallback
	configRev  int64
	bucketName string
}

func (pop *pingOp) Cancel() {
	for _, subop := range pop.subops {
		subop.op.Cancel()
	}
}

func (pop *pingOp) handledOneLocked() {
	remaining := atomic.AddInt32(&pop.remaining, -1)
	if remaining == 0 {
		pop.callback(&PingResult{
			ConfigRev: pop.configRev,
			Services:  pop.results,
		}, nil)
	}
}

// PingOptions encapsulates the parameters for a PingKvEx operation.
type PingOptions struct {
	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
	Deadline     time.Time
	ServiceTypes []ServiceType // Defaults to KV only.
}

// PingResult encapsulates the result of a PingKvEx operation.
type PingResult struct {
	ConfigRev int64
	Services  map[ServiceType][]EndpointPingResult
}

// MemdConnInfo represents information we know about a particular
// memcached connection reported in a diagnostics report.
type MemdConnInfo struct {
	LocalAddr    string
	RemoteAddr   string
	LastActivity time.Time
	Scope        string
	ID           string
}

// DiagnosticInfo is returned by the Diagnostics method and includes
// information about the overall health of the clients connections.
type DiagnosticInfo struct {
	ConfigRev int64
	MemdConns []MemdConnInfo
}
