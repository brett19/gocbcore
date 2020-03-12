package gocbcore

type configManager struct {
	useSSL      bool
	networkType string

	currentConfig *routeConfig

	cfgChangeWatchers []routeConfigWatch
	invalidWatcher    func()
}

type configManagerProperties struct {
	UseSSL      bool
	NetworkType string
}

type routeConfigWatch func(config *routeConfig)

func newConfigManager(props configManagerProperties, cfgChangeWatchers []routeConfigWatch, invalidCfgWatcher func()) *configManager {
	return &configManager{
		useSSL:            props.UseSSL,
		networkType:       props.NetworkType,
		cfgChangeWatchers: cfgChangeWatchers,
		invalidWatcher:    invalidCfgWatcher,
		currentConfig: &routeConfig{
			revID: -1,
		},
	}
}

func (cm *configManager) OnNewConfig(cfg *cfgBucket) {
	routeCfg := cfg.BuildRouteConfig(cm.useSSL, cm.networkType, false)
	if !routeCfg.IsValid() {
		// This will trigger something upstream to react, i.e. agent to shutdown.
		cm.invalidWatcher()
		return
	}

	// There's something wrong with this route config so don't send it to the watchers.
	if !cm.updateRouteConfig(routeCfg) {
		return
	}

	for _, watcher := range cm.cfgChangeWatchers {
		watcher(routeCfg)
	}
}

func (cm *configManager) OnFirstRouteConfig(config *cfgBucket, srcServer string) bool {
	routeCfg := cm.buildFirstRouteConfig(config, srcServer)
	logDebugf("Using network type %s for connections", cm.networkType)
	if !routeCfg.IsValid() {
		logDebugf("Configuration was deemed invalid %+v", routeCfg)
		return false
	}

	// There's something wrong with this route config so don't send it to the watchers
	// and inform the caller that it was bad.
	if !cm.updateRouteConfig(routeCfg) {
		return false
	}

	for _, watcher := range cm.cfgChangeWatchers {
		watcher(routeCfg)
	}

	return true
}

// We should never be receiving concurrent updates and nothing should be accessing
// our internal route config so we shouldn't need to lock here.
func (cm *configManager) updateRouteConfig(cfg *routeConfig) bool {
	oldCfg := cm.currentConfig

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

	cm.currentConfig = cfg

	return true
}

func (cm *configManager) buildFirstRouteConfig(config *cfgBucket, srcServer string) *routeConfig {
	if cm.networkType != "" && cm.networkType != "auto" {
		return config.BuildRouteConfig(cm.useSSL, cm.networkType, true)
	}

	defaultRouteConfig := config.BuildRouteConfig(cm.useSSL, "default", true)

	// First we check if the source server is from the defaults list
	srcInDefaultConfig := false
	for _, endpoint := range defaultRouteConfig.kvServerList {
		if endpoint == srcServer {
			srcInDefaultConfig = true
		}
	}
	for _, endpoint := range defaultRouteConfig.mgmtEpList {
		if endpoint == srcServer {
			srcInDefaultConfig = true
		}
	}
	if srcInDefaultConfig {
		cm.networkType = "default"
		return defaultRouteConfig
	}

	// Next lets see if we have an external config, if so, default to that
	externalRouteCfg := config.BuildRouteConfig(cm.useSSL, "external", true)
	if externalRouteCfg.IsValid() {
		cm.networkType = "external"
		return externalRouteCfg
	}

	// If all else fails, default to the implicit default config
	cm.networkType = "default"
	return defaultRouteConfig
}

func (cm *configManager) NetworkType() string {
	return cm.networkType
}
