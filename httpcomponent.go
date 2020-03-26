package gocbcore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type httpComponent struct {
	cli                  *http.Client
	muxer                *httpMux
	auth                 AuthProvider
	userAgent            string
	tracer               *tracerComponent
	defaultRetryStrategy RetryStrategy
}

type httpComponentProps struct {
	UserAgent            string
	DefaultRetryStrategy RetryStrategy
}

func newHTTPComponent(props httpComponentProps, cli *http.Client, muxer *httpMux, auth AuthProvider,
	tracer *tracerComponent) *httpComponent {
	return &httpComponent{
		cli:                  cli,
		muxer:                muxer,
		auth:                 auth,
		userAgent:            props.UserAgent,
		defaultRetryStrategy: props.DefaultRetryStrategy,
		tracer:               tracer,
	}
}

func (hc *httpComponent) Close() {
	if tsport, ok := hc.cli.Transport.(*http.Transport); ok {
		tsport.CloseIdleConnections()
	} else {
		logDebugf("Could not close idle connections for transport")
	}
}

func (hc *httpComponent) DoHTTPRequest(req *HTTPRequest) (*HTTPResponse, error) {
	tracer := hc.tracer.CreateOpTrace("http", req.TraceContext)
	defer tracer.Finish()

	retryStrategy := hc.defaultRetryStrategy
	if req.RetryStrategy != nil {
		retryStrategy = req.RetryStrategy
	}

	ireq := &httpRequest{
		Service:          req.Service,
		Endpoint:         req.Endpoint,
		Method:           req.Method,
		Path:             req.Path,
		Headers:          req.Headers,
		ContentType:      req.ContentType,
		Username:         req.Username,
		Password:         req.Password,
		Body:             req.Body,
		IsIdempotent:     req.IsIdempotent,
		UniqueID:         req.UniqueID,
		Deadline:         req.Deadline,
		RetryStrategy:    retryStrategy,
		RootTraceContext: tracer.RootContext(),
	}

	resp, err := hc.DoInternalHTTPRequest(ireq)
	if err != nil {
		return nil, wrapHTTPError(ireq, err)
	}

	return resp, nil
}

func (hc *httpComponent) DoInternalHTTPRequest(req *httpRequest) (*HTTPResponse, error) {
	if req.Service == MemdService {
		return nil, errInvalidService
	}

	// Identify an endpoint to use for the request
	endpoint := req.Endpoint
	if endpoint == "" {
		var err error
		switch req.Service {
		case MgmtService:
			endpoint, err = hc.getMgmtEp()
		case CapiService:
			endpoint, err = hc.getCapiEp()
		case N1qlService:
			endpoint, err = hc.getN1qlEp()
		case FtsService:
			endpoint, err = hc.getFtsEp()
		case CbasService:
			endpoint, err = hc.getCbasEp()
		}
		if err != nil {
			return nil, err
		}

		req.Endpoint = endpoint
	}

	// Generate a request URI
	reqURI := endpoint + req.Path

	// Create a new request
	hreq, err := http.NewRequest(req.Method, reqURI, nil)
	if err != nil {
		return nil, err
	}

	// This creates a context that has a parent with no cancel function. As such WithCancel will not setup any
	// extra go routines and we only need to call cancel on (non-timeout) failure.
	ctx := context.Background()
	ctx, ctxCancel := context.WithCancel(ctx)

	// This is easy to do with a bool and a defer than to ensure that we cancel after every error.
	doneCh := make(chan struct{}, 1)
	querySuccess := false
	defer func() {
		doneCh <- struct{}{}
		if !querySuccess {
			ctxCancel()
		}
	}()

	// Having no deadline is a legitimate case.
	if !req.Deadline.IsZero() {
		go func() {
			select {
			case <-time.After(req.Deadline.Sub(time.Now())):
				ctxCancel()
			case <-doneCh:
			}
		}()
	}

	// Lets add our context to the httpRequest
	hreq = hreq.WithContext(ctx)

	body := req.Body

	// Inject credentials into the request
	if req.Username != "" || req.Password != "" {
		hreq.SetBasicAuth(req.Username, req.Password)
	} else {
		creds, err := hc.auth.Credentials(AuthCredsRequest{
			Service:  req.Service,
			Endpoint: endpoint,
		})
		if err != nil {
			return nil, err
		}

		if req.Service == N1qlService || req.Service == CbasService ||
			req.Service == FtsService {
			// Handle service which support multi-bucket authentication using
			// injection into the body of the request.
			if len(creds) == 1 {
				hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
			} else {
				body = injectJSONCreds(body, creds)
			}
		} else {
			if len(creds) != 1 {
				return nil, errInvalidCredentials
			}

			hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
		}
	}

	hreq.Body = ioutil.NopCloser(bytes.NewReader(body))

	if req.ContentType != "" {
		hreq.Header.Set("Content-Type", req.ContentType)
	} else {
		hreq.Header.Set("Content-Type", "application/json")
	}
	for key, val := range req.Headers {
		hreq.Header.Set(key, val)
	}

	var uniqueID string
	if req.UniqueID != "" {
		uniqueID = req.UniqueID
	} else {
		uniqueID = uuid.New().String()
	}
	hreq.Header.Set("User-Agent", clientInfoString(uniqueID, hc.userAgent))

	for {
		// dSpan := agent.tracer.StartSpan("dispatch_to_server", req.RootTraceContext).
		// 	SetTag("retry", req.retryCount)
		hresp, err := hc.cli.Do(hreq)
		// dSpan.Finish()
		if err != nil {
			// Because we have to hijack the context for our own timeout specification
			// purposes and we manually cancel it, we need to perform some translation here if we hijacked it.
			if errors.Is(err, context.Canceled) {
				err = errAmbiguousTimeout
			}

			if !req.IsIdempotent {
				return nil, err
			}

			isUserError := false
			isUserError = isUserError || errors.Is(err, context.DeadlineExceeded)
			isUserError = isUserError || errors.Is(err, context.Canceled)
			isUserError = isUserError || errors.Is(err, ErrTimeout)
			if isUserError {
				return nil, err
			}

			var retryReason RetryReason
			if errors.Is(err, io.ErrUnexpectedEOF) {
				retryReason = SocketCloseInFlightRetryReason
			}

			if retryReason == nil {
				return nil, err
			}

			shouldRetry, retryTime := retryOrchMaybeRetry(req, retryReason)
			if !shouldRetry {
				return nil, err
			}

			select {
			case <-time.After(retryTime.Sub(time.Now())):
				// continue!
			case <-time.After(req.Deadline.Sub(time.Now())):
				if errors.Is(err, context.DeadlineExceeded) {
					err = errUnambiguousTimeout
				}

				return nil, err
			}

			continue
		}

		respOut := HTTPResponse{
			Endpoint:   endpoint,
			StatusCode: hresp.StatusCode,
			Body:       hresp.Body,
		}

		querySuccess = true

		return &respOut, nil
	}
}

func (hc *httpComponent) getMgmtEp() (string, error) {
	mgmtEps := hc.muxer.MgmtEps()
	if len(mgmtEps) == 0 {
		return "", errServiceNotAvailable
	}
	return mgmtEps[rand.Intn(len(mgmtEps))], nil
}

func (hc *httpComponent) getCapiEp() (string, error) {
	capiEps := hc.muxer.CapiEps()
	if len(capiEps) == 0 {
		return "", errServiceNotAvailable
	}
	return capiEps[rand.Intn(len(capiEps))], nil
}

func (hc *httpComponent) getN1qlEp() (string, error) {
	n1qlEps := hc.muxer.N1qlEps()
	if len(n1qlEps) == 0 {
		return "", errServiceNotAvailable
	}
	return n1qlEps[rand.Intn(len(n1qlEps))], nil
}

func (hc *httpComponent) getFtsEp() (string, error) {
	ftsEps := hc.muxer.FtsEps()
	if len(ftsEps) == 0 {
		return "", errServiceNotAvailable
	}
	return ftsEps[rand.Intn(len(ftsEps))], nil
}

func (hc *httpComponent) getCbasEp() (string, error) {
	cbasEps := hc.muxer.CbasEps()
	if len(cbasEps) == 0 {
		return "", errServiceNotAvailable
	}
	return cbasEps[rand.Intn(len(cbasEps))], nil
}

func injectJSONCreds(body []byte, creds []UserPassPair) []byte {
	var props map[string]json.RawMessage
	err := json.Unmarshal(body, &props)
	if err == nil {
		if _, ok := props["creds"]; ok {
			// Early out if the user has already passed a set of credentials.
			return body
		}

		jsonCreds, err := json.Marshal(creds)
		if err == nil {
			props["creds"] = json.RawMessage(jsonCreds)

			newBody, err := json.Marshal(props)
			if err == nil {
				return newBody
			}
		}
	}

	return body
}
