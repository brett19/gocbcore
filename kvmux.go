package gocbcore

import (
	"errors"
	"sort"
	"sync/atomic"
	"unsafe"
)

type kvMux struct {
	muxPtr unsafe.Pointer

	queueSize   int
	poolSize    int
	getClientFn memdGetClientFunc
	breakerCfg  CircuitBreakerConfig
}

func newKVMux(qSize, poolSize int, getClientFn memdGetClientFunc) *kvMux {
	return &kvMux{
		queueSize:   qSize,
		poolSize:    poolSize,
		getClientFn: getClientFn,
	}
}

func (mux *kvMux) Get() *memdClientMux {
	return (*memdClientMux)(atomic.LoadPointer(&mux.muxPtr))
}

func (mux *kvMux) Update(old, new *memdClientMux) bool {
	if new == nil {
		logErrorf("Attempted to update to nil memdClientMux")
		return false
	}

	if old != nil {
		return atomic.CompareAndSwapPointer(&mux.muxPtr, unsafe.Pointer(old), unsafe.Pointer(new))
	}

	if atomic.SwapPointer(&mux.muxPtr, unsafe.Pointer(new)) != nil {
		logErrorf("Updated from nil attempted on initialized memdClientMux")
		return false
	}

	return true
}

func (mux *kvMux) Clear() *memdClientMux {
	val := atomic.SwapPointer(&mux.muxPtr, nil)
	return (*memdClientMux)(val)
}

//  This method MUST NEVER BLOCK due to its use from various contention points.
func (mux *kvMux) ApplyRoutingConfig(cfg *routeConfig) {
	oldClientMux := mux.Get()

	var newClientMux *memdClientMux
	if cfg.IsGCCCPConfig() {
		newClientMux = newMemdClientMux(cfg, 1, mux.queueSize, mux.getClientFn)
	} else {
		newClientMux = newMemdClientMux(cfg, mux.poolSize, mux.queueSize, mux.getClientFn)
	}

	logDebugf("Switching kv mux routing data (update)...")
	logDebugf("New Routing Data:\n%s", cfg.DebugString())

	// Attempt to atomically update the routing data
	if !mux.Update(oldClientMux, newClientMux) {
		logErrorf("Someone preempted the config update, skipping update")
		return
	}

	if oldClientMux == nil {
		// This is a new agent so there is no existing muxer.  We can
		// simply start the new muxer.
		newClientMux.Start()
	} else {
		// Get the new muxer to takeover the pipelines from the older one
		newClientMux.Takeover(oldClientMux)

		// Gather all the requests from all the old pipelines and then
		//  sort and redispatch them (which will use the new pipelines)
		var requestList []*memdQRequest
		oldClientMux.Drain(func(req *memdQRequest) {
			requestList = append(requestList, req)
		})

		sort.Sort(memdQRequestSorter(requestList))

		// TODO: don't forget these
		for _, req := range requestList {
			// 	agent.stopCmdTrace(req)
			mux.RequeueDirect(req, false)
		}
	}
}

func (mux *kvMux) ConfigUUID() string {
	clientMux := mux.Get()
	if clientMux == nil {
		return ""
	}
	return clientMux.uuid
}

func (mux *kvMux) KeyToVbucket(key []byte) uint16 {
	clientMux := mux.Get()
	if clientMux == nil || clientMux.vbMap == nil {
		return 0
	}

	return clientMux.vbMap.VbucketByKey(key)
}

func (mux *kvMux) KeyToServer(key []byte, replicaIdx uint32) int {
	clientMux := mux.Get()
	if clientMux.vbMap != nil {
		serverIdx, err := clientMux.vbMap.NodeByKey(key, replicaIdx)
		if err != nil {
			return -1
		}

		return serverIdx
	}

	if clientMux.ketamaMap != nil {
		serverIdx, err := clientMux.ketamaMap.NodeByKey(key)
		if err != nil {
			return -1
		}

		return serverIdx
	}

	return -1
}

func (mux *kvMux) VbucketToServer(vbID uint16, replicaIdx uint32) int {
	clientMux := mux.Get()
	if clientMux == nil || clientMux.vbMap == nil {
		return 0
	}

	if clientMux.vbMap == nil {
		return -1
	}

	serverIdx, err := clientMux.vbMap.NodeByVbucket(vbID, replicaIdx)
	if err != nil {
		return -1
	}

	return serverIdx
}

func (mux *kvMux) NumReplicas() int {
	clientMux := mux.Get()
	if clientMux == nil {
		return 0
	}

	if clientMux.vbMap == nil {
		return 0
	}

	return clientMux.vbMap.NumReplicas()
}

func (mux *kvMux) BucketType() bucketType {
	clientMux := mux.Get()
	if clientMux == nil {
		return bktTypeInvalid
	}

	return clientMux.bktType
}

func (mux *kvMux) VbucketsOnServer(index int) []uint16 {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil
	}

	if clientMux.vbMap == nil {
		return nil
	}

	vbList := clientMux.vbMap.VbucketsByServer(0)

	if len(vbList) <= index {
		// Invalid server index
		return nil
	}

	return vbList[index]
}

func (mux *kvMux) SupportsGCCCP() bool {
	clientMux := mux.Get()
	return clientMux.BucketType() == bktTypeNone
}

func (mux *kvMux) NumVBuckets() int {
	clientMux := mux.Get()
	return clientMux.vbMap.NumVbuckets()
}

func (mux *kvMux) NumPipelines() int {
	clientMux := mux.Get()
	return clientMux.NumPipelines()
}

func (mux *kvMux) RouteRequest(req *memdQRequest) (*memdPipeline, error) {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil, errShutdown
	}

	var srvIdx int
	repIdx := req.ReplicaIdx

	// Route to specific server
	if repIdx < 0 {
		srvIdx = -repIdx - 1
	} else {
		var err error

		if clientMux.bktType == bktTypeCouchbase {
			if req.Key != nil {
				req.Vbucket = clientMux.vbMap.VbucketByKey(req.Key)
			}

			srvIdx, err = clientMux.vbMap.NodeByVbucket(req.Vbucket, uint32(repIdx))

			if err != nil {
				return nil, err
			}
		} else if clientMux.bktType == bktTypeMemcached {
			if repIdx > 0 {
				// Error. Memcached buckets don't understand replicas!
				return nil, errInvalidReplica
			}

			if len(req.Key) == 0 {
				// Non-broadcast keyless Memcached bucket request
				return nil, errInvalidArgument
			}

			srvIdx, err = clientMux.ketamaMap.NodeByKey(req.Key)
			if err != nil {
				return nil, err
			}
		}
	}

	return clientMux.GetPipeline(srvIdx), nil
}

func (mux *kvMux) DispatchDirect(req *memdQRequest) error {
	// agent.startCmdTrace(req)

	for {
		pipeline, err := mux.RouteRequest(req)
		if err != nil {
			return err
		}

		err = pipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err == errPipelineFull {
			return errOverload
		} else if err != nil {
			return err
		}

		break
	}

	return nil
}

func (mux *kvMux) RequeueDirect(req *memdQRequest, isRetry bool) {
	// agent.startCmdTrace(req)
	handleError := func(err error) {
		// We only want to log an error on retries if the error isn't cancelled.
		if !isRetry || (isRetry && !errors.Is(err, ErrRequestCanceled)) {
			logErrorf("Reschedule failed, failing request (%s)", err)
		}

		req.tryCallback(nil, err)
	}

	logDebugf("Request being requeued, Opaque=%d", req.Opaque)

	for {
		pipeline, err := mux.RouteRequest(req)
		if err != nil {
			handleError(err)
			return
		}

		err = pipeline.RequeueRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err != nil {
			handleError(err)
			return
		}

		break
	}
}

func (mux *kvMux) DispatchDirectToAddress(req *memdQRequest, address string) error {
	// agent.startCmdTrace(req)

	// We set the ReplicaIdx to a negative number to ensure it is not redispatched
	// and we check that it was 0 to begin with to ensure it wasn't miss-used.
	if req.ReplicaIdx != 0 {
		return errInvalidReplica
	}
	req.ReplicaIdx = -999999999

	for {
		clientMux := mux.Get()
		if clientMux == nil {
			return errShutdown
		}

		var foundPipeline *memdPipeline
		for _, pipeline := range clientMux.pipelines {
			if pipeline.Address() == address {
				foundPipeline = pipeline
				break
			}
		}

		if foundPipeline == nil {
			return errInvalidServer
		}

		err := foundPipeline.SendRequest(req)
		if err == errPipelineClosed {
			continue
		} else if err == errPipelineFull {
			return errOverload
		} else if err != nil {
			return err
		}

		break
	}

	return nil
}

func (mux *kvMux) Close() error {
	clientMux := mux.Clear()

	// Shut down the client multiplexer which will close all its queues
	// effectively causing all the clients to shut down.
	muxErr := clientMux.Close()

	// Drain all the pipelines and error their requests, then
	//  drain the dead queue and error those requests.
	clientMux.Drain(func(req *memdQRequest) {
		req.tryCallback(nil, errShutdown)
	})

	return muxErr
}

type pipelineIterator struct {
	pipelines []*memdPipeline

	idx        int
	iterations int
	len        int
}

func (mux *kvMux) PipelineIterator() (*pipelineIterator, error) {
	clientMux := mux.Get()
	if clientMux == nil {
		return nil, errShutdown
	}

	p := clientMux.pipelines
	return &pipelineIterator{
		pipelines: p,
		len:       len(p),
	}, nil
}

func (pi *pipelineIterator) Len() int {
	return pi.len
}

func (pi *pipelineIterator) Offset(offset int) {
	pi.idx = offset
}

func (pi *pipelineIterator) Next() *memdPipeline {
	if pi.iterations == pi.len {
		return nil
	}

	pi.iterations++
	pi.idx = (pi.idx + 1) % pi.len
	return pi.pipelines[pi.idx]

}
