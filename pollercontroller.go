package gocbcore

type pollerController struct {
	activeController configController
}

type configController interface {
	Pause(paused bool)
	Done() chan struct{}
	Stop()
}

func (pc *pollerController) StartCCCPLooper(properties cccpPollerProperties, muxer *kvMux, watcher func(config *cfgBucket)) {
	cccpController := newCCCPConfigController(properties, muxer, watcher)
	go cccpController.DoLoop()

	pc.activeController = cccpController
}

func (pc *pollerController) StartHTTPLooper(bucketName string, props httpPollerProperties, muxer *httpMux,
	watcher func(config *cfgBucket), firstCfgFn func(*cfgBucket, string, error) bool) {
	controller := newHTTPConfigController(bucketName, props, muxer, watcher)
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
