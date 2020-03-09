package gocbcore

type pollerController struct {
	activeController configController
}

type configController interface {
	Pause(paused bool)
	Done() chan struct{}
	Stop()
}

func (pc *pollerController) StartCCCPLooper(properties cccpPollerProperties, watcher func(config *cfgBucket), getMuxer func() (*routeConfig, *memdClientMux)) {
	cccpController := newCCCPConfigController(properties, watcher, getMuxer)
	go cccpController.DoLoop()

	pc.activeController = cccpController
}

func (pc *pollerController) StartHTTPLooper(props httpPollerProperties, watcher func(config *cfgBucket), getCfg func() (*routeConfig, *memdClientMux),
	firstCfgFn func(*cfgBucket, string, error) bool) {
	controller := newHTTPConfigController(props, watcher, getCfg)
	go controller.DoLoop(firstCfgFn)

	pc.activeController = controller
}

func (pc *pollerController) PauseLooper(paused bool) {
	pc.activeController.Pause(paused)
}

func (pc *pollerController) StopLooper() {
	if pc.activeController != nil {
		pc.activeController.Stop()
	}
}

func (pc *pollerController) Done() chan struct{} {
	if pc.activeController == nil {
		return nil
	}
	return pc.activeController.Done()
}
