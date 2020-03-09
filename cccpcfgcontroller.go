package gocbcore

import (
	"math/rand"
	"time"
)

type cccpConfigController struct {
	getMuxer           func() (*routeConfig, *memdClientMux)
	watcher            func(config *cfgBucket)
	confCccpPollPeriod time.Duration
	confCccpMaxWait    time.Duration
	closeNotify        chan struct{}

	// Used exclusively for testing to overcome GOCBC-780. It allows a test to pause the cccp looper preventing
	// unwanted requests from being sent to the mock once it has been setup for error map testing.
	looperPauseSig chan bool

	looperStopSig chan struct{}
	looperDoneSig chan struct{}
}

func newCCCPConfigController(props cccpPollerProperties, watcher func(config *cfgBucket), getMuxer func() (*routeConfig, *memdClientMux)) *cccpConfigController {
	return &cccpConfigController{
		getMuxer:           getMuxer,
		watcher:            watcher,
		confCccpPollPeriod: props.confCccpPollPeriod,
		confCccpMaxWait:    props.confCccpMaxWait,
		closeNotify:        props.closeNotify,

		looperPauseSig: make(chan bool),
		looperStopSig:  make(chan struct{}),
		looperDoneSig:  make(chan struct{}),
	}
}

type cccpPollerProperties struct {
	confCccpPollPeriod time.Duration
	confCccpMaxWait    time.Duration
	closeNotify        chan struct{}
}

func (ccc *cccpConfigController) Pause(paused bool) {
	ccc.looperPauseSig <- paused
}

func (ccc *cccpConfigController) Stop() {
	ccc.looperStopSig <- struct{}{}
}

func (ccc *cccpConfigController) Done() chan struct{} {
	return ccc.looperDoneSig
}

func (ccc *cccpConfigController) DoLoop() {
	tickTime := ccc.confCccpPollPeriod
	paused := false

	logDebugf("CCCP Looper starting.")

	nodeIdx := -1

Looper:
	for {
		// Wait for either the agent to be shut down, or our tick time to expire
		select {
		case <-ccc.looperStopSig:
			break Looper
		case pause := <-ccc.looperPauseSig:
			paused = pause
		case <-time.After(tickTime):
		case <-ccc.closeNotify:
		}

		if paused {
			continue
		}

		cfg, muxer := ccc.getMuxer()
		if cfg == nil {
			break
		}

		numNodes := muxer.NumPipelines()
		if numNodes == 0 {
			logDebugf("CCCPPOLL: No nodes available to poll")
			continue
		}

		if nodeIdx < 0 {
			nodeIdx = rand.Intn(numNodes)
		}

		var foundConfig *cfgBucket
		for nodeOff := 0; nodeOff < numNodes; nodeOff++ {
			nodeIdx = (nodeIdx + 1) % numNodes

			pipeline := muxer.GetPipeline(nodeIdx)

			cccpBytes, err := ccc.getClusterConfig(pipeline)
			if err != nil {
				logDebugf("CCCPPOLL: Failed to retrieve CCCP config. %v", err)
				continue
			}

			hostName, err := hostFromHostPort(pipeline.Address())
			if err != nil {
				logErrorf("CCCPPOLL: Failed to parse source address. %v", err)
				continue
			}

			bk, err := parseConfig(cccpBytes, hostName)
			if err != nil {
				logDebugf("CCCPPOLL: Failed to parse CCCP config. %v", err)
				continue
			}

			foundConfig = bk
			break
		}

		if foundConfig == nil {
			logDebugf("CCCPPOLL: Failed to retrieve config from any node.")
			continue
		}

		logDebugf("CCCPPOLL: Received new config")
		ccc.watcher(foundConfig)
	}

	close(ccc.looperDoneSig)
}

func (ccc *cccpConfigController) getClusterConfig(pipeline *memdPipeline) (cfgOut []byte, errOut error) {
	signal := make(chan struct{})
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:  reqMagic,
			Opcode: cmdGetClusterConfig,
		},
		Callback: func(resp *memdQResponse, _ *memdQRequest, err error) {
			if resp != nil {
				cfgOut = resp.memdPacket.Value
			}
			errOut = err
			signal <- struct{}{}
		},
		RetryStrategy: newFailFastRetryStrategy(),
	}
	err := pipeline.SendRequest(req)
	if err != nil {
		return nil, err
	}

	timeoutTmr := AcquireTimer(ccc.confCccpMaxWait)
	select {
	case <-signal:
		ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		ReleaseTimer(timeoutTmr, true)
		req.Cancel(errAmbiguousTimeout)
		<-signal
		return
	}
}
