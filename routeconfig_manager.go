package gocbcore

import (
	"errors"
	"sort"
	"sync"
)

type routeConfigManager struct {
	routingConfig *routeConfig
	configLock    sync.Mutex
	clientMux     *memdClientMux

	queueSize   int
	poolSize    int
	getClientFn memdGetClientFunc
	breakerCfg  CircuitBreakerConfig
}

type memdQRequestSorter []*memdQRequest

func (list memdQRequestSorter) Len() int {
	return len(list)
}

func (list memdQRequestSorter) Less(i, j int) bool {
	return list[i].dispatchTime.Before(list[j].dispatchTime)
}

func (list memdQRequestSorter) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func newRouteConfigManager(qSize, poolSize int, getClientFn memdGetClientFunc, breakerCfg CircuitBreakerConfig) *routeConfigManager {
	return &routeConfigManager{
		queueSize:   qSize,
		poolSize:    poolSize,
		getClientFn: getClientFn,
		breakerCfg:  breakerCfg,
		routingConfig: &routeConfig{
			revID: -1,
		},
	}
}

func (rcm *routeConfigManager) OnRouteCfgChange(routeCfg *routeConfig) bool {
	return rcm.applyRoutingConfig(routeCfg)
}

// The route config and muxer are a pair and must always be used as a unit.
func (rcm *routeConfigManager) Get() (*routeConfig, *memdClientMux) {
	rcm.configLock.Lock()
	cfg := rcm.routingConfig
	mux := rcm.clientMux
	rcm.configLock.Unlock()

	return cfg, mux
}

// Accepts a cfgBucket object representing a cluster configuration and rebuilds the server list
//  along with any routing information for the Client.  Passing no config will refresh the existing one.
//  This method MUST NEVER BLOCK due to its use from various contention points.
func (rcm *routeConfigManager) applyRoutingConfig(cfg *routeConfig) bool {
	// Only a single thing can modify the config at any time
	rcm.configLock.Lock()
	defer rcm.configLock.Unlock()

	oldCfg := rcm.routingConfig

	// Check some basic things to ensure consistency!
	if oldCfg.revID > -1 {
		if (cfg.vbMap == nil) != (oldCfg.vbMap == nil) {
			logErrorf("Received a configuration with a different number of vbuckets.  Ignoring.")
			return false
		}

		if cfg.vbMap != nil && cfg.vbMap.NumVbuckets() != oldCfg.vbMap.NumVbuckets() {
			logErrorf("Received a configuration with a different number of vbuckets.  Ignoring.")
			return false
		}
	}

	// Check that the new config data is newer than the current one, in the case where we've done a select bucket
	// against an existing connection then the revisions could be the same. In that case the configuration still
	// needs to be applied.
	if cfg.revID == 0 {
		logDebugf("Unversioned configuration data, switching.")
	} else if cfg.bktType != oldCfg.bktType {
		logDebugf("Configuration data changed bucket type, switching.")
	} else if cfg.revID == oldCfg.revID {
		logDebugf("Ignoring configuration with identical revision number")
		return false
	} else if cfg.revID < oldCfg.revID {
		logDebugf("Ignoring new configuration as it has an older revision id")
		return false
	}

	oldClientMux := rcm.clientMux
	var newClientMux *memdClientMux
	if cfg.IsGCCCPConfig() {
		newClientMux = newMemdClientMux(cfg.kvServerList, 1, rcm.queueSize, rcm.getClientFn, rcm.breakerCfg)
	} else {
		newClientMux = newMemdClientMux(cfg.kvServerList, rcm.poolSize, rcm.queueSize, rcm.getClientFn, rcm.breakerCfg)
	}

	rcm.routingConfig = cfg
	rcm.clientMux = newClientMux

	logDebugf("Switching routing data (update)...")
	logDebugf("New Routing Data:\n%s", cfg.DebugString())

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
		newClientMux.Drain(func(req *memdQRequest) {
			requestList = append(requestList, req)
		})

		sort.Sort(memdQRequestSorter(requestList))

		// TODO: don't forget these
		// for _, req := range requestList {
		// 	agent.stopCmdTrace(req)
		// 	agent.requeueDirect(req, false)
		// }
	}

	return true
}

func (rcm *routeConfigManager) ConfigUUID() string {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return ""
	}
	return cfg.uuid
}

func (rcm *routeConfigManager) KeyToVbucket(key []byte) uint16 {
	cfg, _ := rcm.Get()
	if cfg == nil || cfg.vbMap == nil {
		return 0
	}

	return cfg.vbMap.VbucketByKey(key)
}

func (rcm *routeConfigManager) KeyToServer(key []byte, replicaIdx uint32) int {
	cfg, _ := rcm.Get()
	if cfg.vbMap != nil {
		serverIdx, err := cfg.vbMap.NodeByKey(key, replicaIdx)
		if err != nil {
			return -1
		}

		return serverIdx
	}

	if cfg.ketamaMap != nil {
		serverIdx, err := cfg.ketamaMap.NodeByKey(key)
		if err != nil {
			return -1
		}

		return serverIdx
	}

	return -1
}

func (rcm *routeConfigManager) VbucketToServer(vbID uint16, replicaIdx uint32) int {
	cfg, _ := rcm.Get()
	if cfg == nil || cfg.vbMap == nil {
		return 0
	}

	if cfg.vbMap == nil {
		return -1
	}

	serverIdx, err := cfg.vbMap.NodeByVbucket(vbID, replicaIdx)
	if err != nil {
		return -1
	}

	return serverIdx
}

func (rcm *routeConfigManager) NumReplicas() int {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return 0
	}

	if cfg.vbMap == nil {
		return 0
	}

	return cfg.vbMap.NumReplicas()
}

func (rcm *routeConfigManager) BucketType() bucketType {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return bktTypeInvalid
	}

	return cfg.bktType
}

func (rcm *routeConfigManager) VbucketsOnServer(index int) []uint16 {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return nil
	}

	if cfg.vbMap == nil {
		return nil
	}

	vbList := cfg.vbMap.VbucketsByServer(0)

	if len(vbList) <= index {
		// Invalid server index
		return nil
	}

	return vbList[index]
}

func (rcm *routeConfigManager) CapiEps() []string {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return nil
	}

	return cfg.capiEpList
}

func (rcm *routeConfigManager) MgmtEps() []string {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return nil
	}

	return cfg.mgmtEpList
}

func (rcm *routeConfigManager) N1qlEps() []string {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return nil
	}

	return cfg.n1qlEpList
}

func (rcm *routeConfigManager) CbasEps() []string {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return nil
	}

	return cfg.cbasEpList
}

func (rcm *routeConfigManager) FtsEps() []string {
	cfg, _ := rcm.Get()
	if cfg == nil {
		return nil
	}

	return cfg.ftsEpList
}

func (rcm *routeConfigManager) SupportsGCCCP() bool {
	cfg, _ := rcm.Get()
	return cfg.IsGCCCPConfig()
}

func (rcm *routeConfigManager) NumVBuckets() int {
	cfg, _ := rcm.Get()
	return cfg.vbMap.NumVbuckets()
}

func (rcm *routeConfigManager) NumPipelines() int {
	_, mux := rcm.Get()
	return mux.NumPipelines()
}

func (rcm *routeConfigManager) RouteRequest(req *memdQRequest) (*memdPipeline, error) {
	cfg, mux := rcm.Get()
	if cfg == nil {
		return nil, errShutdown
	}

	var srvIdx int
	repIdx := req.ReplicaIdx

	// Route to specific server
	if repIdx < 0 {
		srvIdx = -repIdx - 1
	} else {
		var err error

		if cfg.bktType == bktTypeCouchbase {
			if req.Key != nil {
				req.Vbucket = cfg.vbMap.VbucketByKey(req.Key)
			}

			srvIdx, err = cfg.vbMap.NodeByVbucket(req.Vbucket, uint32(repIdx))

			if err != nil {
				return nil, err
			}
		} else if cfg.bktType == bktTypeMemcached {
			if repIdx > 0 {
				// Error. Memcached buckets don't understand replicas!
				return nil, errInvalidReplica
			}

			if len(req.Key) == 0 {
				// Non-broadcast keyless Memcached bucket request
				return nil, errInvalidArgument
			}

			srvIdx, err = cfg.ketamaMap.NodeByKey(req.Key)
			if err != nil {
				return nil, err
			}
		}
	}

	return mux.GetPipeline(srvIdx), nil
}

func (rcm *routeConfigManager) DispatchDirect(req *memdQRequest) error {
	// agent.startCmdTrace(req)

	for {
		pipeline, err := rcm.RouteRequest(req)
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

func (rcm *routeConfigManager) DispatchDirectToAddress(req *memdQRequest, address string) error {
	// agent.startCmdTrace(req)

	// We set the ReplicaIdx to a negative number to ensure it is not redispatched
	// and we check that it was 0 to begin with to ensure it wasn't miss-used.
	if req.ReplicaIdx != 0 {
		return errInvalidReplica
	}
	req.ReplicaIdx = -999999999

	for {
		cfg, mux := rcm.Get()
		if cfg == nil {
			return errShutdown
		}

		var foundPipeline *memdPipeline
		for _, pipeline := range mux.pipelines {
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

func (rcm *routeConfigManager) RequeueDirect(req *memdQRequest, isRetry bool) {
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
		pipeline, err := rcm.RouteRequest(req)
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

func (rcm *routeConfigManager) Close() error {
	rcm.configLock.Lock()
	rcm.routingConfig = nil

	// Shut down the client multiplexer which will close all its queues
	// effectively causing all the clients to shut down.
	muxErr := rcm.clientMux.Close()

	// Drain all the pipelines and error their requests, then
	//  drain the dead queue and error those requests.
	rcm.clientMux.Drain(func(req *memdQRequest) {
		req.tryCallback(nil, errShutdown)
	})
	rcm.configLock.Unlock()

	return muxErr
}
