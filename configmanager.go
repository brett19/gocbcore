package gocbcore

import "sync/atomic"

type configManager struct {
	useSSL      bool
	networkType string

	firstConfig bool

	cfgChangeWatcher routeConfigWatch
	invalidWatcher   func()

	clusterCapabilities uint32
}

type configManagerProperties struct {
	UseSSL      bool
	NetworkType string
}

// routeConfigWatch will receive a route config and then reply whether or not that route config was used.
type routeConfigWatch func(config *routeConfig) bool

func newConfigManager(props configManagerProperties, cfgChangeWatcher routeConfigWatch, invalidCfgWatcher func()) *configManager {
	return &configManager{
		useSSL:           props.UseSSL,
		networkType:      props.NetworkType,
		cfgChangeWatcher: cfgChangeWatcher,
		invalidWatcher:   invalidCfgWatcher,
	}
}

func (cm *configManager) OnNewConfig(cfg *cfgBucket) {
	routeCfg := cfg.BuildRouteConfig(cm.useSSL, cm.networkType, false)
	if !routeCfg.IsValid() {
		// This will trigger something upstream to react, i.e. agent to shutdown.
		cm.invalidWatcher()
		return
	}
	used := cm.cfgChangeWatcher(routeCfg)
	if used {
		cm.updateClusterCapabilities(cfg)
	}
}

func (cm *configManager) OnFirstRouteConfig(config *cfgBucket, srcServer string) bool {
	routeCfg := cm.buildFirstRouteConfig(config, srcServer)
	logDebugf("Using network type %s for connections", cm.networkType)
	if !routeCfg.IsValid() {
		logDebugf("Configuration was deemed invalid %+v", routeCfg)
		return false
	}

	cm.updateClusterCapabilities(config)
	return cm.cfgChangeWatcher(routeCfg)
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

// SupportsClusterCapability returns whether or not the cluster supports a given capability.
func (cm *configManager) SupportsClusterCapability(capability ClusterCapability) bool {
	capabilities := ClusterCapability(atomic.LoadUint32(&cm.clusterCapabilities))

	return capabilities&capability != 0
}

// TODO: this should likely be its own manager
func (cm *configManager) updateClusterCapabilities(cfg *cfgBucket) {
	capabilities := cm.buildClusterCapabilities(cfg)
	if capabilities == 0 {
		return
	}

	atomic.StoreUint32(&cm.clusterCapabilities, uint32(capabilities))
}

func (cm *configManager) buildClusterCapabilities(cfg *cfgBucket) ClusterCapability {
	caps := cfg.ClusterCaps()
	capsVer := cfg.ClusterCapsVer()
	if capsVer == nil || len(capsVer) == 0 || caps == nil {
		return 0
	}

	var agentCapabilities ClusterCapability
	if capsVer[0] == 1 {
		for category, catCapabilities := range caps {
			switch category {
			case "n1ql":
				for _, capability := range catCapabilities {
					switch capability {
					case "enhancedPreparedStatements":
						agentCapabilities |= ClusterCapabilityEnhancedPreparedStatements
					}
				}
			}
		}
	}

	return agentCapabilities
}

func (cm *configManager) NetworkType() string {
	return cm.networkType
}
