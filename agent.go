// Package gocbcore implements methods for low-level communication
// with a Couchbase Server cluster.
package gocbcore

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Agent represents the base client handling connections to a Couchbase Server.
// This is used internally by the higher level classes for communicating with the cluster,
// it can also be used to perform more advanced operations with a cluster.
type Agent struct {
	clientID             string
	bucketName           string
	tlsConfig            *tls.Config
	initFn               memdInitFunc
	defaultRetryStrategy RetryStrategy

	pollerController *pollerController
	kvMux            *kvMux
	httpMux          *httpMux

	cfgManager   *configManagementComponent
	errMap       *errMapComponent
	collections  *collectionsComponent
	tracer       *tracerComponent
	http         *httpComponent
	diagnostics  *diagnosticsComponent
	crud         *crudComponent
	stats        *statsComponent
	n1ql         *n1qlQueryComponent
	analytics    *analyticsQueryComponent
	search       *searchQueryComponent
	views        *viewQueryComponent
	zombieLogger *zombieLoggerComponent
}

// !!!!UNSURE WHY THESE EXIST!!!!
// ServerConnectTimeout gets the timeout for each server connection, including all authentication steps.
// func (agent *Agent) ServerConnectTimeout() time.Duration {
// 	return agent.kvConnectTimeout
// }
//
// // SetServerConnectTimeout sets the timeout for each server connection.
// func (agent *Agent) SetServerConnectTimeout(timeout time.Duration) {
// 	agent.kvConnectTimeout = timeout
// }

// HTTPClient returns a pre-configured HTTP Client for communicating with
// Couchbase Server.  You must still specify authentication information
// for any dispatched requests.
func (agent *Agent) HTTPClient() *http.Client {
	return agent.http.cli
}

// AuthFunc is invoked by the agent to authenticate a client. This function returns two channels to allow for for multi-stage
// authentication processes (such as SCRAM). The continue callback should be called when further asynchronous bootstrapping
// requests (such as select bucket) can be sent. The completed callback should be called when authentication is completed,
// or failed. It should contain any error that occurred. If completed is called before continue then continue will be called
// first internally, the success value will be determined by whether or not an error is present.
type AuthFunc func(client AuthClient, deadline time.Time, continueCb func(), completedCb func(error)) error

// authFunc wraps AuthFunc to provide a better to the user.
type authFunc func() (completedCh chan BytesAndError, continueCh chan bool, err error)

type authFuncHandler func(client AuthClient, deadline time.Time, mechanism AuthMechanism) authFunc

// CreateAgent creates an agent for performing normal operations.
func CreateAgent(config *AgentConfig) (*Agent, error) {
	initFn := func(client *memdClient, deadline time.Time) error {
		return nil
	}

	return createAgent(config, initFn)
}

func createAgent(config *AgentConfig, initFn memdInitFunc) (*Agent, error) {
	logInfof("SDK Version: gocbcore/%s", goCbCoreVersionStr)
	logInfof("Creating new agent: %+v", config)

	var tlsConfig *tls.Config
	if config.UseTLS {
		tlsConfig = &tls.Config{
			RootCAs: config.TLSRootCAs,
			GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return config.Auth.Certificate(AuthCertRequest{})
			},
			InsecureSkipVerify: config.TLSSkipVerify,
		}
	}

	httpTransport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConns:        config.HTTPMaxIdleConns,
		MaxIdleConnsPerHost: config.HTTPMaxIdleConnsPerHost,
		IdleConnTimeout:     config.HTTPIdleConnectionTimeout,
	}

	httpCli := &http.Client{
		Transport: httpTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// All that we're doing here is setting auth on any redirects.
			// For that reason we can just pull it off the oldest (first) request.
			if len(via) >= 10 {
				// Just duplicate the default behaviour for maximum redirects.
				return errors.New("stopped after 10 redirects")
			}

			oldest := via[0]
			auth := oldest.Header.Get("Authorization")
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}

			return nil
		},
	}

	tracer := config.Tracer
	if tracer == nil {
		tracer = noopTracer{}
	}
	tracerCmpt := newTracerComponent(tracer, config.BucketName, config.NoRootTraceSpans)

	c := &Agent{
		clientID:   formatCbUID(randomCbUID()),
		bucketName: config.BucketName,
		tlsConfig:  tlsConfig,
		initFn:     initFn,
		tracer:     tracerCmpt,

		defaultRetryStrategy: config.DefaultRetryStrategy,

		errMap: newErrMapManager(config.BucketName),
	}

	circuitBreakerConfig := config.CircuitBreakerConfig
	auth := config.Auth
	userAgent := config.UserAgent
	useMutationTokens := config.UseMutationTokens
	disableDecompression := config.DisableDecompression
	useCompression := config.UseCompression
	useCollections := config.UseCollections
	compressionMinSize := 32
	compressionMinRatio := 0.83
	useDurations := config.UseDurations

	kvConnectTimeout := 7000 * time.Millisecond
	if config.KVConnectTimeout > 0 {
		kvConnectTimeout = config.KVConnectTimeout
	}

	serverWaitTimeout := 5 * time.Second

	kvPoolSize := 1
	if config.KvPoolSize > 0 {
		kvPoolSize = config.KvPoolSize
	}

	maxQueueSize := 2048
	if config.MaxQueueSize > 0 {
		maxQueueSize = config.MaxQueueSize
	}

	confHTTPRetryDelay := 10 * time.Second
	if config.HTTPRetryDelay > 0 {
		confHTTPRetryDelay = config.HTTPRetryDelay
	}

	confHTTPRedialPeriod := 10 * time.Second
	if config.HTTPRedialPeriod > 0 {
		confHTTPRedialPeriod = config.HTTPRedialPeriod
	}

	confCccpMaxWait := 3 * time.Second
	if config.CccpMaxWait > 0 {
		confCccpMaxWait = config.CccpMaxWait
	}

	confCccpPollPeriod := 2500 * time.Millisecond
	if config.CccpPollPeriod > 0 {
		confCccpPollPeriod = config.CccpPollPeriod
	}

	if config.CompressionMinSize > 0 {
		compressionMinSize = config.CompressionMinSize
	}
	if config.CompressionMinRatio > 0 {
		compressionMinRatio = config.CompressionMinRatio
		if compressionMinRatio >= 1.0 {
			compressionMinRatio = 1.0
		}
	}
	if c.defaultRetryStrategy == nil {
		c.defaultRetryStrategy = newFailFastRetryStrategy()
	}
	authMechanisms := []AuthMechanism{
		ScramSha512AuthMechanism,
		ScramSha256AuthMechanism,
		ScramSha1AuthMechanism}

	// PLAIN authentication is only supported over TLS
	if config.UseTLS {
		authMechanisms = append(authMechanisms, PlainAuthMechanism)
	}

	authHandler := buildAuthHandler(auth)

	var httpEpList []string
	for _, hostPort := range config.HTTPAddrs {
		if !c.IsSecure() {
			httpEpList = append(httpEpList, fmt.Sprintf("http://%s", hostPort))
		} else {
			httpEpList = append(httpEpList, fmt.Sprintf("https://%s", hostPort))
		}
	}

	if config.UseZombieLogger {
		zombieLoggerInterval := 10 * time.Second
		zombieLoggerSampleSize := 10
		if config.ZombieLoggerInterval > 0 {
			zombieLoggerInterval = config.ZombieLoggerInterval
		}
		if config.ZombieLoggerSampleSize > 0 {
			zombieLoggerSampleSize = config.ZombieLoggerSampleSize
		}

		c.zombieLogger = newZombieLoggerComponent(zombieLoggerInterval, zombieLoggerSampleSize)
		go c.zombieLogger.Start()
	}

	c.cfgManager = newConfigManager(
		configManagerProperties{
			NetworkType:  config.NetworkType,
			UseSSL:       config.UseTLS,
			SrcMemdAddrs: config.MemdAddrs,
			SrcHTTPAddrs: httpEpList,
		},
		c.onInvalidConfig,
	)

	dialer := newMemdClientCreatorComponent(
		memdClientDialerProps{
			ServerWaitTimeout:    serverWaitTimeout,
			KVConnectTimeout:     kvConnectTimeout,
			ClientID:             c.clientID,
			TLSConfig:            c.tlsConfig,
			CompressionMinSize:   compressionMinSize,
			CompressionMinRatio:  compressionMinRatio,
			DisableDecompression: disableDecompression,
		},
		bootstrapProps{
			HelloProps: helloProps{
				CollectionsEnabled:    useCollections,
				MutationTokensEnabled: useMutationTokens,
				CompressionEnabled:    useCompression,
				DurationsEnabled:      useDurations,
			},
			Bucket:         c.bucketName,
			UserAgent:      userAgent,
			AuthMechanisms: authMechanisms,
			AuthHandler:    authHandler,
			ErrMapManager:  c.errMap,
		},
		circuitBreakerConfig,
		c.zombieLogger,
		c.tracer,
		initFn,
	)
	c.kvMux = newKVMux(
		kvMuxProps{
			QueueSize:          maxQueueSize,
			PoolSize:           kvPoolSize,
			CollectionsEnabled: useCollections,
		},
		c.cfgManager,
		c.errMap,
		c.tracer,
		dialer,
	)
	c.collections = newCollectionIDManager(
		collectionIDProps{
			MaxQueueSize:         config.MaxQueueSize,
			DefaultRetryStrategy: c.defaultRetryStrategy,
		},
		c.kvMux,
		c.tracer,
	)
	c.httpMux = newHTTPMux(circuitBreakerConfig, c.cfgManager)
	c.http = newHTTPComponent(
		httpComponentProps{
			UserAgent:            userAgent,
			DefaultRetryStrategy: c.defaultRetryStrategy,
		},
		httpCli,
		c.httpMux,
		auth,
		c.tracer,
	)

	// If a user connects on a non default http port then we won't translate the address into memd addresses.
	// If this happens then we should instruct the poller controller to skip straight to http polling. The cccp poller
	// will not see no pipelines as a reason to fallback, it will just keep looping until it finds pipelines.
	var cccpPoller *cccpConfigController
	if len(config.MemdAddrs) > 0 {
		cccpPoller = newCCCPConfigController(
			cccpPollerProperties{
				confCccpMaxWait:    confCccpMaxWait,
				confCccpPollPeriod: confCccpPollPeriod,
			},
			c.kvMux,
			c.cfgManager,
		)
	}
	c.pollerController = newPollerController(
		cccpPoller,
		newHTTPConfigController(
			c.bucketName,
			httpPollerProperties{
				httpComponent:        c.http,
				confHTTPRetryDelay:   confHTTPRetryDelay,
				confHTTPRedialPeriod: confHTTPRedialPeriod,
			},
			c.httpMux,
			c.cfgManager,
		),
	)

	c.crud = newCRUDComponent(c.collections, c.defaultRetryStrategy, c.tracer, c.errMap)
	c.stats = newStatsComponent(c.kvMux, c.defaultRetryStrategy, c.tracer)
	c.n1ql = newN1QLQueryComponent(c.http, c.cfgManager, c.tracer)
	c.analytics = newAnalyticsQueryComponent(c.http, c.tracer)
	c.search = newSearchQueryComponent(c.http, c.tracer)
	c.views = newViewQueryComponent(c.http, c.tracer)
	c.diagnostics = newDiagnosticsComponent(c.kvMux, c.httpMux, c.http, c.bucketName)

	// Kick everything off.
	cfg := &routeConfig{
		kvServerList: config.MemdAddrs,
		mgmtEpList:   httpEpList,
		revID:        -1,
	}

	c.httpMux.OnNewRouteConfig(cfg)
	c.kvMux.OnNewRouteConfig(cfg)

	go c.pollerController.Start()

	return c, nil
}

func buildAuthHandler(auth AuthProvider) authFuncHandler {
	return func(client AuthClient, deadline time.Time, mechanism AuthMechanism) authFunc {
		creds, err := getKvAuthCreds(auth, client.Address())
		if err != nil {
			return nil
		}

		if creds.Username != "" || creds.Password != "" {
			return func() (chan BytesAndError, chan bool, error) {
				continueCh := make(chan bool, 1)
				completedCh := make(chan BytesAndError, 1)
				hasContinued := int32(0)
				callErr := saslMethod(mechanism, creds.Username, creds.Password, client, deadline, func() {
					// hasContinued should never be 1 here but let's guard against it.
					if atomic.CompareAndSwapInt32(&hasContinued, 0, 1) {
						continueCh <- true
					}
				}, func(err error) {
					if atomic.CompareAndSwapInt32(&hasContinued, 0, 1) {
						sendContinue := true
						if err != nil {
							sendContinue = false
						}
						continueCh <- sendContinue
					}
					completedCh <- BytesAndError{Err: err}
				})
				if callErr != nil {
					return nil, nil, err
				}
				return completedCh, continueCh, nil
			}
		}

		return nil
	}
}

func (agent *Agent) disconnectClient(client *memdClient) {
	err := client.Close()
	if err != nil {
		logErrorf("Failed to shut down client connection (%s)", err)
	}
}

func (agent *Agent) onInvalidConfig() {
	err := agent.Close()
	if err != nil {
		logErrorf("Invalid config caused agent close failure (%s)", err)
	}
}

// Close shuts down the agent, disconnecting from all servers and failing
// any outstanding operations with ErrShutdown.
func (agent *Agent) Close() error {
	routeCloseErr := agent.kvMux.Close()
	agent.pollerController.Stop()

	if agent.zombieLogger != nil {
		agent.zombieLogger.Stop()
	}

	// Wait for our external looper goroutines to finish, note that if the
	// specific looper wasn't used, it will be a nil value otherwise it
	// will be an open channel till its closed to signal completion.
	<-agent.pollerController.Done()

	// Close the transports so that they don't hold open goroutines.
	agent.http.Close()

	return routeCloseErr
}

// BucketUUID returns the UUID of the bucket we are connected to.
func (agent *Agent) BucketUUID() string {
	return agent.kvMux.ConfigUUID()
}

// KeyToVbucket translates a particular key to its assigned vbucket.
func (agent *Agent) KeyToVbucket(key []byte) uint16 {
	return agent.kvMux.KeyToVbucket(key)
}

// KeyToServer translates a particular key to its assigned server index.
func (agent *Agent) KeyToServer(key []byte, replicaIdx uint32) int {
	return agent.kvMux.KeyToServer(key, replicaIdx)
}

// VbucketToServer returns the server index for a particular vbucket.
func (agent *Agent) VbucketToServer(vbID uint16, replicaIdx uint32) int {
	return agent.kvMux.VbucketToServer(vbID, replicaIdx)
}

// NumVbuckets returns the number of VBuckets configured on the
// connected cluster.
func (agent *Agent) NumVbuckets() int {
	return agent.kvMux.NumVBuckets()
}

// NumReplicas returns the number of replicas configured on the
// connected cluster.
func (agent *Agent) NumReplicas() int {
	return agent.kvMux.NumReplicas()
}

// NumServers returns the number of servers accessible for K/V.
func (agent *Agent) NumServers() int {
	return agent.kvMux.NumPipelines()
}

// VbucketsOnServer returns the list of VBuckets for a server.
func (agent *Agent) VbucketsOnServer(index int) []uint16 {
	return agent.kvMux.VbucketsOnServer(index)
}

// ClientID returns the unique id for this agent
func (agent *Agent) ClientID() string {
	return agent.clientID
}

// CapiEps returns all the available endpoints for performing
// map-reduce queries.
func (agent *Agent) CapiEps() []string {
	return agent.httpMux.CapiEps()
}

// MgmtEps returns all the available endpoints for performing
// management queries.
func (agent *Agent) MgmtEps() []string {
	return agent.httpMux.MgmtEps()
}

// N1qlEps returns all the available endpoints for performing
// N1QL queries.
func (agent *Agent) N1qlEps() []string {
	return agent.httpMux.N1qlEps()
}

// FtsEps returns all the available endpoints for performing
// FTS queries.
func (agent *Agent) FtsEps() []string {
	return agent.httpMux.FtsEps()
}

// CbasEps returns all the available endpoints for performing
// CBAS queries.
func (agent *Agent) CbasEps() []string {
	return agent.httpMux.CbasEps()
}

// HasCollectionsSupport verifies whether or not collections are available on the agent.
func (agent *Agent) HasCollectionsSupport() bool {
	return agent.kvMux.SupportsCollections()
}

// IsSecure returns whether this client is connected via SSL.
func (agent *Agent) IsSecure() bool {
	return agent.tlsConfig != nil
}

// UsingGCCCP returns whether or not the Agent is currently using GCCCP polling.
func (agent *Agent) UsingGCCCP() bool {
	return agent.kvMux.SupportsGCCCP()
}

// WaitUntilReady returns whether or not the Agent has seen a valid cluster config.
func (agent *Agent) WaitUntilReady(deadline time.Time, opts WaitUntilReadyOptions, cb WaitUntilReadyCallback) (PendingOp, error) {
	return agent.diagnostics.WaitUntilReady(deadline, opts, cb)
}
