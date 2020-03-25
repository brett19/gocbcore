package gocbcore

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// N1QLRowReader providers access to the rows of a n1ql query
type N1QLRowReader struct {
	streamer *queryStreamer
}

// NextRow reads the next rows bytes from the stream
func (q *N1QLRowReader) NextRow() []byte {
	return q.streamer.NextRow()
}

// Err returns any errors that occurred during streaming.
func (q N1QLRowReader) Err() error {
	return q.streamer.Err()
}

// MetaData fetches the non-row bytes streamed in the response.
func (q *N1QLRowReader) MetaData() ([]byte, error) {
	return q.streamer.MetaData()
}

// Close immediately shuts down the connection
func (q *N1QLRowReader) Close() error {
	return q.streamer.Close()
}

// PreparedName returns the name of the prepared statement created when using enhanced prepared statements.
// If the prepared name has not been seen on the stream then this will return an error.
// Volatile: This API is subject to change.
func (q N1QLRowReader) PreparedName() (string, error) {
	val := q.streamer.EarlyMetadata("prepared")
	if val == nil {
		return "", wrapN1QLError(nil, "", errors.New("prepared name not found in metadata"))
	}

	var name string
	err := json.Unmarshal(val, &name)
	if err != nil {
		return "", wrapN1QLError(nil, "", errors.New("failed to parse prepared name"))
	}

	return name, nil
}

// N1QLQueryOptions represents the various options available for a n1ql query.
type N1QLQueryOptions struct {
	Payload       []byte
	RetryStrategy RetryStrategy
	Deadline      time.Time

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

func wrapN1QLError(req *httpRequest, statement string, err error) *N1QLError {
	if err == nil {
		err = errors.New("query error")
	}

	ierr := &N1QLError{
		InnerError: err,
	}

	if req != nil {
		ierr.Endpoint = req.Endpoint
		ierr.ClientContextID = req.UniqueID
		ierr.RetryAttempts = req.RetryAttempts()
		ierr.RetryReasons = req.RetryReasons()
	}

	ierr.Statement = statement

	return ierr
}

type jsonN1QLError struct {
	Code uint32 `json:"code"`
	Msg  string `json:"msg"`
}

type jsonN1QLErrorResponse struct {
	Errors []jsonN1QLError
}

func parseN1QLError(req *httpRequest, statement string, resp *HTTPResponse) *N1QLError {
	var err error
	var errorDescs []N1QLErrorDesc

	respBody, readErr := ioutil.ReadAll(resp.Body)
	if readErr == nil {
		var respParse jsonN1QLErrorResponse
		parseErr := json.Unmarshal(respBody, &respParse)
		if parseErr == nil {

			for _, jsonErr := range respParse.Errors {
				errorDescs = append(errorDescs, N1QLErrorDesc{
					Code:    jsonErr.Code,
					Message: jsonErr.Msg,
				})
			}
		}
	}

	if len(errorDescs) >= 1 {
		firstErr := errorDescs[0]
		errCode := firstErr.Code
		errCodeGroup := errCode / 1000

		if errCode == 3000 {
			err = errParsingFailure
		}
		if errCode == 12009 {
			err = errCasMismatch
		}
		if errCodeGroup == 5 {
			err = errInternalServerFailure
		}
		if errCodeGroup == 10 {
			err = errAuthenticationFailure
		}

		if errCodeGroup == 4 {
			err = errPlanningFailure
		}
		if errCodeGroup == 12 || errCodeGroup == 14 && errCode != 12004 && errCode != 12016 {
			err = errIndexFailure
		}
		if errCode == 4040 || errCode == 4050 || errCode == 4060 || errCode == 4070 || errCode == 4080 || errCode == 4090 {
			err = errPreparedStatementFailure
		}
	}

	errOut := wrapN1QLError(req, statement, err)
	errOut.Errors = errorDescs
	return errOut
}

type n1qlQueryComponent struct {
	httpComponent *httpComponent
	cfgMgr        *configManager

	queryCache map[string]*n1qlQueryCacheEntry
	cacheLock  sync.RWMutex

	enhancedPreparedSupported uint32
}

type n1qlQueryCacheEntry struct {
	enhanced    bool
	name        string
	encodedPlan string
}

type n1qlJSONPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name        string `json:"name"`
}

func newN1QLQueryComponent(httpComponent *httpComponent, cfgMgr *configManager) *n1qlQueryComponent {
	nqc := &n1qlQueryComponent{
		httpComponent: httpComponent,
		cfgMgr:        cfgMgr,
		queryCache:    make(map[string]*n1qlQueryCacheEntry),
	}
	cfgMgr.AddConfigWatcher(nqc)

	return nqc
}

func (nqc *n1qlQueryComponent) OnNewRouteConfig(cfg *routeConfig) {
	if atomic.LoadUint32(&nqc.enhancedPreparedSupported) == 0 &&
		cfg.ContainsClusterCapability(1, "n1ql", "enhancedPreparedStatements") {
		// Once supported this can't be unsupported
		atomic.StoreUint32(&nqc.enhancedPreparedSupported, 1)
		nqc.cacheLock.Lock()
		nqc.queryCache = make(map[string]*n1qlQueryCacheEntry)
		nqc.cacheLock.Unlock()
	}
}

// N1QLQuery executes a N1QL query
func (nqc *n1qlQueryComponent) N1QLQuery(opts N1QLQueryOptions) (*N1QLRowReader, error) {
	tracer := nqc.httpComponent.CreateOpTrace("N1QLQuery", opts.TraceContext)
	defer tracer.Finish()

	var payloadMap map[string]interface{}
	err := json.Unmarshal(opts.Payload, &payloadMap)
	if err != nil {
		return nil, wrapN1QLError(nil, "", wrapError(err, "expected a JSON payload"))
	}

	statement := getMapValueString(payloadMap, "statement", "")
	clientContextID := getMapValueString(payloadMap, "client_context_id", "")
	readOnly := getMapValueBool(payloadMap, "readonly", false)

	ireq := &httpRequest{
		Service:          N1qlService,
		Method:           "POST",
		Path:             "/query/service",
		IsIdempotent:     readOnly,
		UniqueID:         clientContextID,
		Deadline:         opts.Deadline,
		RetryStrategy:    opts.RetryStrategy,
		RootTraceContext: tracer.RootContext(),
	}

	return nqc.execute(statement, ireq, payloadMap)
}

// PreparedN1QLQuery executes a prepared N1QL query
func (nqc *n1qlQueryComponent) PreparedN1QLQuery(opts N1QLQueryOptions) (*N1QLRowReader, error) {
	tracer := nqc.httpComponent.CreateOpTrace("N1QLQuery", opts.TraceContext)
	defer tracer.Finish()

	if atomic.LoadUint32(&nqc.enhancedPreparedSupported) == 1 {
		return nqc.executeEnhPrepared(opts, tracer)
	}

	return nqc.executeOldPrepared(opts, tracer)
}

func (nqc *n1qlQueryComponent) executeEnhPrepared(opts N1QLQueryOptions, tracer *opTracer) (*N1QLRowReader, error) {
	var payloadMap map[string]interface{}
	err := json.Unmarshal(opts.Payload, &payloadMap)
	if err != nil {
		return nil, wrapN1QLError(nil, "", wrapError(err, "expected a JSON payload"))
	}

	statement := getMapValueString(payloadMap, "statement", "")
	clientContextID := getMapValueString(payloadMap, "client_context_id", "")
	readOnly := getMapValueBool(payloadMap, "readonly", false)

	nqc.cacheLock.RLock()
	cachedStmt := nqc.queryCache[statement]
	nqc.cacheLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(payloadMap, "statement")
		payloadMap["prepared"] = cachedStmt.name

		ireq := &httpRequest{
			Service:      N1qlService,
			Method:       "POST",
			Path:         "/query/service",
			IsIdempotent: readOnly,
			UniqueID:     clientContextID,
			Deadline:     opts.Deadline,
			// We need to not retry this request.
			RetryStrategy:    newFailFastRetryStrategy(),
			RootTraceContext: tracer.RootContext(),
		}

		results, err := nqc.execute("", ireq, payloadMap)
		if err == nil {
			return results, nil
		}
		// if we fail to send the prepared statement name then retry a PREPARE.
		delete(payloadMap, "prepared")
	}

	payloadMap["statement"] = "PREPARE " + statement
	payloadMap["auto_execute"] = true

	ireq := &httpRequest{
		Service:          N1qlService,
		Method:           "POST",
		Path:             "/query/service",
		IsIdempotent:     readOnly,
		UniqueID:         clientContextID,
		Deadline:         opts.Deadline,
		RetryStrategy:    opts.RetryStrategy,
		RootTraceContext: tracer.RootContext(),
	}

	results, err := nqc.execute("", ireq, payloadMap)
	if err != nil {
		return nil, err
	}

	preparedName, err := results.PreparedName()
	if err != nil {
		logWarnf("Failed to read prepared name from result: %s", err)
		return results, nil
	}

	cachedStmt = &n1qlQueryCacheEntry{}
	cachedStmt.name = preparedName
	cachedStmt.enhanced = true

	nqc.cacheLock.Lock()
	nqc.queryCache[statement] = cachedStmt
	nqc.cacheLock.Unlock()

	return results, nil
}

func (nqc *n1qlQueryComponent) executeOldPrepared(opts N1QLQueryOptions, tracer *opTracer) (*N1QLRowReader, error) {
	var payloadMap map[string]interface{}
	err := json.Unmarshal(opts.Payload, &payloadMap)
	if err != nil {
		return nil, wrapN1QLError(nil, "", wrapError(err, "expected a JSON payload"))
	}

	statement := getMapValueString(payloadMap, "statement", "")
	clientContextID := getMapValueString(payloadMap, "client_context_id", "")
	readOnly := getMapValueBool(payloadMap, "readonly", false)

	nqc.cacheLock.RLock()
	cachedStmt := nqc.queryCache[statement]
	nqc.cacheLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(payloadMap, "statement")
		payloadMap["prepared"] = cachedStmt.name
		payloadMap["encoded_plan"] = cachedStmt.encodedPlan

		ireq := &httpRequest{
			Service:          N1qlService,
			Method:           "POST",
			Path:             "/query/service",
			IsIdempotent:     readOnly,
			UniqueID:         clientContextID,
			Deadline:         opts.Deadline,
			RetryStrategy:    opts.RetryStrategy,
			RootTraceContext: tracer.RootContext(),
		}

		results, err := nqc.execute("", ireq, payloadMap)
		if err == nil {
			return results, nil
		}
		// if we fail to send the prepared statement name then retry a PREPARE.
	}

	delete(payloadMap, "prepared")
	delete(payloadMap, "encoded_plan")
	delete(payloadMap, "auto_execute")
	prepStatement := "PREPARE " + statement
	payloadMap["statement"] = prepStatement

	ireq := &httpRequest{
		Service:          N1qlService,
		Method:           "POST",
		Path:             "/query/service",
		IsIdempotent:     readOnly,
		UniqueID:         clientContextID,
		Deadline:         opts.Deadline,
		RetryStrategy:    opts.RetryStrategy,
		RootTraceContext: tracer.RootContext(),
	}

	cacheRes, err := nqc.execute(prepStatement, ireq, payloadMap)
	if err != nil {
		return nil, err
	}

	b := cacheRes.NextRow()
	if b == nil {
		return nil, wrapN1QLError(ireq, statement, errCliInternalError)
	}

	var prepData n1qlJSONPrepData
	err = json.Unmarshal(b, &prepData)
	if err != nil {
		return nil, wrapN1QLError(ireq, statement, err)
	}

	cachedStmt = &n1qlQueryCacheEntry{}
	cachedStmt.name = prepData.Name
	cachedStmt.encodedPlan = prepData.EncodedPlan

	nqc.cacheLock.Lock()
	nqc.queryCache[statement] = cachedStmt
	nqc.cacheLock.Unlock()

	// Attempt to execute our cached query plan
	delete(payloadMap, "statement")
	payloadMap["prepared"] = cachedStmt.name
	payloadMap["encoded_plan"] = cachedStmt.encodedPlan

	return nqc.execute(cachedStmt.encodedPlan, ireq, payloadMap)
}

func (nqc *n1qlQueryComponent) execute(statement string, ireq *httpRequest, payloadMap map[string]interface{}) (*N1QLRowReader, error) {
ExecuteLoop:
	for {
		{ // Produce an updated payload with the appropriate timeout
			timeoutLeft := ireq.Deadline.Sub(time.Now())
			payloadMap["timeout"] = timeoutLeft.String()

			newPayload, err := json.Marshal(payloadMap)
			if err != nil {
				return nil, wrapN1QLError(nil, "", wrapError(err, "failed to produce payload"))
			}
			ireq.Body = newPayload
		}

		resp, err := nqc.httpComponent.DoInternalHTTPRequest(ireq)
		if err != nil {
			// execHTTPRequest will handle retrying due to in-flight socket close based
			// on whether or not IsIdempotent is set on the httpRequest
			return nil, wrapN1QLError(ireq, statement, err)
		}

		if resp.StatusCode != 200 {
			n1qlErr := parseN1QLError(ireq, statement, resp)

			var retryReason RetryReason
			if len(n1qlErr.Errors) >= 1 {
				firstErrDesc := n1qlErr.Errors[0]

				if firstErrDesc.Code == 4040 {
					retryReason = QueryPreparedStatementFailureRetryReason
				} else if firstErrDesc.Code == 4050 {
					retryReason = QueryPreparedStatementFailureRetryReason
				} else if firstErrDesc.Code == 4070 {
					retryReason = QueryPreparedStatementFailureRetryReason
				} else if strings.Contains(firstErrDesc.Message, "queryport.indexNotFound") {
					retryReason = QueryIndexNotFoundRetryReason
				}
			}

			if retryReason == nil {
				// n1qlErr is already wrapped here
				return nil, n1qlErr
			}

			shouldRetry, retryTime := retryOrchMaybeRetry(ireq, retryReason)
			if !shouldRetry {
				// n1qlErr is already wrapped here
				return nil, n1qlErr
			}

			select {
			case <-time.After(retryTime.Sub(time.Now())):
				continue ExecuteLoop
			case <-time.After(ireq.Deadline.Sub(time.Now())):
				return nil, wrapN1QLError(ireq, statement, errUnambiguousTimeout)
			}
		}

		streamer, err := newQueryStreamer(resp.Body, "results")
		if err != nil {
			return nil, wrapN1QLError(ireq, statement, err)
		}

		return &N1QLRowReader{
			streamer: streamer,
		}, nil
	}
}
