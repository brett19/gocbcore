package gocbcore

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v8/memd"
)

type diagnosticsComponent struct {
	kvMux         *kvMux
	httpMux       *httpMux
	httpComponent *httpComponent
	bucket        string
}

func newDiagnosticsComponent(kvMux *kvMux, httpMux *httpMux, httpComponent *httpComponent, bucket string) *diagnosticsComponent {
	return &diagnosticsComponent{
		kvMux:         kvMux,
		httpMux:       httpMux,
		bucket:        bucket,
		httpComponent: httpComponent,
	}
}

func (dc *diagnosticsComponent) pingHTTPService(epList []string, path string, service ServiceType, op *pingOp,
	deadline time.Time, retryStrat RetryStrategy) {
	for _, ep := range epList {
		atomic.AddInt32(&op.remaining, 1)
		go func(ep string) {
			req := &httpRequest{
				Service:       service,
				Method:        "GET",
				Path:          path,
				Deadline:      deadline,
				RetryStrategy: retryStrat,
				Endpoint:      ep,
				IsIdempotent:  true,
			}
			start := time.Now()
			_, err := dc.httpComponent.DoInternalHTTPRequest(req)
			pingLatency := time.Now().Sub(start)
			state := PingStateOK
			if err != nil {
				if errors.Is(err, ErrTimeout) {
					state = PingStateTimeout
				} else {
					state = PingStateError
				}
			}
			op.lock.Lock()
			op.results[service] = append(op.results[service], EndpointPingResult{
				Endpoint: ep,
				Error:    err,
				Latency:  pingLatency,
				Scope:    op.bucketName,
				ID:       uuid.New().String(),
				State:    state,
			})
			op.handledOneLocked()
			op.lock.Unlock()
		}(ep)
	}
}

func (dc *diagnosticsComponent) pingKV(iter *pipelineSnapshot, op *pingOp, deadline time.Time, retryStrat RetryStrategy) {
	iter.Iterate(0, func(pipeline *memdPipeline) bool {
		serverAddress := pipeline.Address()

		startTime := time.Now()
		handler := func(resp *memdQResponse, req *memdQRequest, err error) {
			pingLatency := time.Now().Sub(startTime)

			state := PingStateOK
			if err != nil {
				if errors.Is(err, ErrTimeout) {
					state = PingStateTimeout
				} else {
					state = PingStateError
				}
			}

			op.lock.Lock()
			op.results[MemdService] = append(op.results[MemdService], EndpointPingResult{
				Endpoint: serverAddress,
				Error:    err,
				Latency:  pingLatency,
				Scope:    op.bucketName,
				ID:       fmt.Sprintf("%p", pipeline),
				State:    state,
			})
			op.handledOneLocked()
			op.lock.Unlock()
		}

		req := &memdQRequest{
			Packet: memd.Packet{
				Magic:    memd.CmdMagicReq,
				Command:  memd.CmdNoop,
				Datatype: 0,
				Cas:      0,
				Key:      nil,
				Value:    nil,
			},
			Callback:      handler,
			RetryStrategy: retryStrat,
		}

		curOp, err := dc.kvMux.DispatchDirectToAddress(req, pipeline)
		if err != nil {
			op.lock.Lock()
			op.results[MemdService] = append(op.results[MemdService], EndpointPingResult{
				Endpoint: redactSystemData(serverAddress),
				Error:    err,
				Latency:  0,
				Scope:    op.bucketName,
			})
			op.lock.Unlock()
			return false
		}

		if !deadline.IsZero() {
			timer := time.AfterFunc(deadline.Sub(time.Now()), func() {
				req.cancelWithCallback(errUnambiguousTimeout)
			})
			req.processingLock.Lock()
			req.Timer = timer
			req.processingLock.Unlock()
		}

		op.lock.Lock()
		op.subops = append(op.subops, pingSubOp{
			endpoint: serverAddress,
			op:       curOp,
		})
		atomic.AddInt32(&op.remaining, 1)
		op.lock.Unlock()

		// We iterate through all pipelines
		return false
	})
}

func (dc *diagnosticsComponent) Ping(opts PingOptions, cb PingCallback) (PendingOp, error) {
	iter, err := dc.kvMux.PipelineSnapshot()
	if err != nil {
		return nil, err
	}

	bucketName := ""
	if dc.bucket != "" {
		bucketName = redactMetaData(dc.bucket)
	}

	serviceTypes := opts.ServiceTypes
	if len(serviceTypes) == 0 {
		serviceTypes = []ServiceType{MemdService}
	}

	op := &pingOp{
		callback:   cb,
		remaining:  1,
		configRev:  iter.RevID(),
		results:    make(map[ServiceType][]EndpointPingResult),
		bucketName: bucketName,
	}

	retryStrat := newFailFastRetryStrategy()

	httpMuxClient := dc.httpMux.Get()
	for _, serviceType := range serviceTypes {
		switch serviceType {
		case MemdService:
			dc.pingKV(iter, op, opts.Deadline, retryStrat)
		case CapiService:
			dc.pingHTTPService(httpMuxClient.cbasEpList, "/", CapiService, op, opts.Deadline, retryStrat)
		case N1qlService:
			dc.pingHTTPService(httpMuxClient.n1qlEpList, "/admin/ping", N1qlService, op, opts.Deadline, retryStrat)
		case FtsService:
			dc.pingHTTPService(httpMuxClient.ftsEpList, "/api/ping", FtsService, op, opts.Deadline, retryStrat)
		case CbasService:
			dc.pingHTTPService(httpMuxClient.cbasEpList, "/admin/ping", CbasService, op, opts.Deadline, retryStrat)
		}
	}

	// We initialized remaining to one to ensure that the callback is not
	// invoked until all of the operations have been dispatched first.  This
	// final handling is to indicate that all operations were dispatched.
	op.lock.Lock()
	op.handledOneLocked()
	op.lock.Unlock()

	return op, nil
}

// Diagnostics returns diagnostics information about the client.
// Mainly containing a list of open connections and their current
// states.
func (dc *diagnosticsComponent) Diagnostics() (*DiagnosticInfo, error) {
	for {
		iter, err := dc.kvMux.PipelineSnapshot()
		if err != nil {
			return nil, err
		}

		var conns []MemdConnInfo

		iter.Iterate(0, func(pipeline *memdPipeline) bool {
			pipeline.clientsLock.Lock()
			for _, pipecli := range pipeline.clients {
				localAddr := ""
				remoteAddr := ""
				var lastActivity time.Time

				pipecli.lock.Lock()
				if pipecli.client != nil {
					localAddr = pipecli.client.LocalAddress()
					remoteAddr = pipecli.client.Address()
					lastActivityUs := atomic.LoadInt64(&pipecli.client.lastActivity)
					if lastActivityUs != 0 {
						lastActivity = time.Unix(0, lastActivityUs)
					}
				}
				pipecli.lock.Unlock()

				conn := MemdConnInfo{
					LocalAddr:    localAddr,
					RemoteAddr:   remoteAddr,
					LastActivity: lastActivity,
					ID:           fmt.Sprintf("%p", pipecli),
				}
				if dc.bucket != "" {
					conn.Scope = redactMetaData(dc.bucket)
				}
				conns = append(conns, conn)
			}
			pipeline.clientsLock.Unlock()
			return false
		})

		endIter, err := dc.kvMux.PipelineSnapshot()
		if err != nil {
			return nil, err
		}
		if iter.RevID() == endIter.RevID() {
			return &DiagnosticInfo{
				ConfigRev: iter.RevID(),
				MemdConns: conns,
			}, nil
		}
	}
}
