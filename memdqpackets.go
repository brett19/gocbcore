package gocbcore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/gocbcore/v9/memd"
)

// The data for a response from a server.  This includes the
// packets data along with some useful meta-data related to
// the response.
type memdQResponse struct {
	memd.Packet

	sourceAddr   string
	sourceConnID string
}

type callback func(*memdQResponse, *memdQRequest, error)

// The data for a request that can be queued with a memdqueueconn,
// and can potentially be rerouted to multiple servers due to
// configuration changes.
type memdQRequest struct {
	memd.Packet

	// Static routing properties
	ReplicaIdx int
	Callback   callback
	Persistent bool

	// This tracks when the request was dispatched so that we can
	//  properly prioritize older requests to try and meet timeout
	//  requirements.
	dispatchTime time.Time

	// This stores a pointer to the server that currently own
	//   this request.  This allows us to remove it from that list
	//   whenever the request is cancelled.
	queuedWith unsafe.Pointer

	// This stores a pointer to the opList that currently is holding
	//  this request.  This allows us to remove it form that list
	//  whenever the request is cancelled
	waitingIn unsafe.Pointer

	// This keeps track of whether the request has been 'completed'
	//  which is synonymous with the callback having been invoked.
	//  This is an integer to allow us to atomically control it.
	isCompleted uint32

	// This is used to lock access to the request when processing
	// a timeout, a response or spans
	processingLock sync.Mutex

	// This stores the number of times that the item has been
	// retried. It is used for various non-linear retry
	// algorithms.
	retryCount uint32

	// This is used to determine what, if any, retry strategy to use
	// when deciding whether to retry the request and calculating
	// any back-off time period.
	RetryStrategy RetryStrategy

	// This is the set of reasons why this request has been retried.
	retryReasons []RetryReason

	// This is the timer which is used for cancellation of the request when deadlines are used.
	Timer *time.Timer

	lastDispatchedTo   string
	lastDispatchedFrom string
	lastConnectionID   string

	RootTraceContext RequestSpanContext
	cmdTraceSpan     RequestSpan
	netTraceSpan     RequestSpan

	CollectionName string
	ScopeName      string
}

func (req *memdQRequest) RetryAttempts() uint32 {
	return atomic.LoadUint32(&req.retryCount)
}

func (req *memdQRequest) retryStrategy() RetryStrategy {
	return req.RetryStrategy
}

func (req *memdQRequest) incrementRetryAttempts() {
	atomic.AddUint32(&req.retryCount, 1)
}

func (req *memdQRequest) Identifier() string {
	return fmt.Sprintf("0x%x", req.Opaque)
}

func (req *memdQRequest) Idempotent() bool {
	_, ok := idempotentOps[req.Command]
	return ok
}

func (req *memdQRequest) RetryReasons() []RetryReason {
	return req.retryReasons
}

func (req *memdQRequest) LocalEndpoint() string {
	return req.lastDispatchedFrom
}

func (req *memdQRequest) RemoteEndpoint() string {
	return req.lastDispatchedTo
}

func (req *memdQRequest) ConnectionID() string {
	return req.lastConnectionID
}

func (req *memdQRequest) addRetryReason(retryReason RetryReason) {
	found := false
	for i := 0; i < len(req.retryReasons); i++ {
		if req.retryReasons[i] == retryReason {
			found = true
			break
		}
	}

	// if idx is out of the range of retryReasons then it wasn't found.
	if !found {
		req.retryReasons = append(req.retryReasons, retryReason)
	}
}

func (req *memdQRequest) tryCallback(resp *memdQResponse, err error) bool {
	if req.Timer != nil {
		req.Timer.Stop()
	}

	if req.Persistent {
		if atomic.LoadUint32(&req.isCompleted) == 0 {
			req.Callback(resp, req, err)
			return true
		}
	} else {
		if atomic.SwapUint32(&req.isCompleted, 1) == 0 {
			req.Callback(resp, req, err)
			return true
		}
	}

	return false
}

func (req *memdQRequest) isCancelled() bool {
	return atomic.LoadUint32(&req.isCompleted) != 0
}

func (req *memdQRequest) internalCancel(err error) bool {
	req.processingLock.Lock()

	if atomic.SwapUint32(&req.isCompleted, 1) != 0 {
		// Someone already completed this request
		req.processingLock.Unlock()
		return false
	}

	if req.Timer != nil {
		// This timer might have already fired and that's how we got here, however we might have also got here
		// via other means so we should always try to stop it.
		req.Timer.Stop()
	}

	queuedWith := (*memdOpQueue)(atomic.LoadPointer(&req.queuedWith))
	if queuedWith != nil {
		queuedWith.Remove(req)
	}

	waitingIn := (*memdClient)(atomic.LoadPointer(&req.waitingIn))
	if waitingIn != nil {
		waitingIn.CancelRequest(req, err)
	}

	cancelReqTrace(req)
	req.processingLock.Unlock()

	return true
}

func (req *memdQRequest) cancelWithCallback(err error) {
	// Try to perform the cancellation, if it succeeds, we call the
	// callback immediately on the users behalf.
	if req.internalCancel(err) {
		req.Callback(nil, req, err)
	}
}

func (req *memdQRequest) Cancel() {
	// Try to perform the cancellation, if it succeeds, we call the
	// callback immediately on the users behalf.
	err := errRequestCanceled
	req.cancelWithCallback(err)
}
