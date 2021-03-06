package gocbcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)

// SearchRowReader providers access to the rows of a view query
type SearchRowReader struct {
	streamer *queryStreamer
}

// NextRow reads the next rows bytes from the stream
func (q *SearchRowReader) NextRow() []byte {
	return q.streamer.NextRow()
}

// Err returns any errors that occured during streaming.
func (q SearchRowReader) Err() error {
	return q.streamer.Err()
}

// MetaData fetches the non-row bytes streamed in the response.
func (q *SearchRowReader) MetaData() ([]byte, error) {
	return q.streamer.MetaData()
}

// Close immediately shuts down the connection
func (q *SearchRowReader) Close() error {
	return q.streamer.Close()
}

// SearchQueryOptions represents the various options available for a search query.
type SearchQueryOptions struct {
	IndexName     string
	Payload       []byte
	RetryStrategy RetryStrategy
	Deadline      time.Time
}

type jsonSearchErrorResponse struct {
	Status string
}

func wrapSearchError(req *httpRequest, resp *HTTPResponse, indexName string, query interface{}, err error) SearchError {
	if err == nil {
		err = errors.New("search error")
	}

	ierr := SearchError{
		InnerError: err,
	}

	if req != nil {
		ierr.Endpoint = req.Endpoint
		ierr.RetryAttempts = req.RetryAttempts()
		ierr.RetryReasons = req.RetryReasons()
	}

	if resp != nil {
		ierr.HTTPResponseCode = resp.StatusCode
	}

	ierr.IndexName = indexName
	ierr.Query = query

	return ierr
}

func parseSearchError(req *httpRequest, indexName string, query interface{}, resp *HTTPResponse) SearchError {
	var err error
	var errMsg string

	respBody, readErr := ioutil.ReadAll(resp.Body)
	if readErr == nil {
		var respParse jsonSearchErrorResponse
		parseErr := json.Unmarshal(respBody, &respParse)
		if parseErr == nil {
			errMsg = respParse.Status
		}
	}

	if resp.StatusCode == 500 {
		err = errInternalServerFailure
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		err = errAuthenticationFailure
	}
	if resp.StatusCode == 400 && strings.Contains(errMsg, "index not found") {
		err = errIndexNotFound
	}

	errOut := wrapSearchError(req, resp, indexName, query, err)
	errOut.ErrorText = errMsg
	return errOut
}

// SearchQuery executes a Search query
func (agent *Agent) SearchQuery(opts SearchQueryOptions) (*SearchRowReader, error) {
	var payloadMap map[string]interface{}
	err := json.Unmarshal(opts.Payload, &payloadMap)
	if err != nil {
		return nil, wrapSearchError(nil, nil, "", nil, wrapError(err, "expected a JSON payload"))
	}

	var ctlMap map[string]interface{}
	if foundCtlMap, ok := payloadMap["ctl"]; ok {
		if coercedCtlMap, ok := foundCtlMap.(map[string]interface{}); ok {
			ctlMap = coercedCtlMap
		} else {
			return nil, wrapSearchError(nil, nil, "", nil,
				wrapError(errInvalidArgument, "expected ctl to be a map"))
		}
	} else {
		ctlMap = make(map[string]interface{})
	}

	indexName := opts.IndexName
	query, _ := payloadMap["query"]

	reqURI := fmt.Sprintf("/api/index/%s/query", opts.IndexName)
	ireq := &httpRequest{
		Service:       FtsService,
		Method:        "POST",
		Path:          reqURI,
		Body:          opts.Payload,
		IsIdempotent:  true,
		Deadline:      opts.Deadline,
		RetryStrategy: opts.RetryStrategy,
	}

ExecuteLoop:
	for {
		{ // Produce an updated payload with the appropriate timeout
			timeoutLeft := ireq.Deadline.Sub(time.Now())

			ctlMap["timeout"] = timeoutLeft / time.Millisecond
			payloadMap["ctl"] = ctlMap

			newPayload, err := json.Marshal(payloadMap)
			if err != nil {
				return nil, wrapSearchError(nil, nil, indexName, query, wrapError(err, "failed to produce payload"))
			}
			ireq.Body = newPayload
		}

		resp, err := agent.execHTTPRequest(ireq)
		if err != nil {
			// execHTTPRequest will handle retrying due to in-flight socket close based
			// on whether or not IsIdempotent is set on the httpRequest
			return nil, wrapSearchError(ireq, nil, indexName, query, err)
		}

		if resp.StatusCode != 200 {
			searchErr := parseSearchError(ireq, indexName, query, resp)

			var retryReason RetryReason
			if searchErr.HTTPResponseCode == 429 {
				retryReason = SearchTooManyRequestsRetryReason
			}

			if retryReason == nil {
				// searchErr is already wrapped here
				return nil, searchErr
			}

			shouldRetry, retryTime := retryOrchMaybeRetry(ireq, retryReason)
			if !shouldRetry {
				// searchErr is already wrapped here
				return nil, searchErr
			}

			select {
			case <-time.After(retryTime.Sub(time.Now())):
				continue ExecuteLoop
			case <-time.After(ireq.Deadline.Sub(time.Now())):
				return nil, wrapSearchError(ireq, nil, indexName, query, errUnambiguousTimeout)
			}
		}

		streamer, err := newQueryStreamer(resp.Body, "results")
		if err != nil {
			return nil, wrapSearchError(ireq, resp, indexName, query, err)
		}

		return &SearchRowReader{
			streamer: streamer,
		}, nil
	}
}
