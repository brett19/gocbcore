package gocbcore

import (
	"encoding/binary"
	"sync"
)

// GetOptions encapsulates the parameters for a GetEx operation.
type GetOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetResult encapsulates the result of a GetEx operation.
type GetResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetExCallback is invoked upon completion of a GetEx operation.
type GetExCallback func(*GetResult, error)

// GetEx retrieves a document.
func (agent *Agent) GetEx(opts GetOptions, cb GetExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, ErrProtocol)
			return
		}

		res := GetResult{}
		res.Value = resp.Value
		res.Flags = binary.BigEndian.Uint32(resp.Extras[0:])
		res.Cas = Cas(resp.Cas)
		res.Datatype = resp.Datatype

		tracer.Finish()
		cb(&res, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGet,
			Datatype:     0,
			Cas:          0,
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// GetAndTouchOptions encapsulates the parameters for a GetAndTouchEx operation.
type GetAndTouchOptions struct {
	Key            []byte
	Expiry         uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetAndTouchResult encapsulates the result of a GetAndTouchEx operation.
type GetAndTouchResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetAndTouchExCallback is invoked upon completion of a GetAndTouchEx operation.
type GetAndTouchExCallback func(*GetAndTouchResult, error)

// GetAndTouchEx retrieves a document and updates its expiry.
func (agent *Agent) GetAndTouchEx(opts GetAndTouchOptions, cb GetAndTouchExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetAndTouchEx", opts.TraceContext)

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, ErrProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		tracer.Finish()
		cb(&GetAndTouchResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Expiry)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGAT,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// GetAndLockOptions encapsulates the parameters for a GetAndLockEx operation.
type GetAndLockOptions struct {
	Key            []byte
	LockTime       uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetAndLockResult encapsulates the result of a GetAndLockEx operation.
type GetAndLockResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetAndLockExCallback is invoked upon completion of a GetAndLockEx operation.
type GetAndLockExCallback func(*GetAndLockResult, error)

// GetAndLockEx retrieves a document and locks it.
func (agent *Agent) GetAndLockEx(opts GetAndLockOptions, cb GetAndLockExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetAndLockEx", opts.TraceContext)

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, ErrProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		tracer.Finish()
		cb(&GetAndLockResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.LockTime)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGetLocked,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// GetAnyReplicaOptions encapsulates the parameters for a GetAnyReplicaEx operation.
type GetAnyReplicaOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetOneReplicaOptions encapsulates the parameters for a GetOneReplicaEx operation.
type GetOneReplicaOptions struct {
	Key            []byte
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy
	ReplicaIdx     int

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetReplicaResult encapsulates the result of a GetReplica operation.
type GetReplicaResult struct {
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
	IsActive bool
}

// GetReplicaExCallback is invoked upon completion of a GetReplica operation.
type GetReplicaExCallback func(*GetReplicaResult, error)

func (agent *Agent) getOneReplica(tracer *opTracer, opts GetOneReplicaOptions, cb GetReplicaExCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			cb(nil, ErrProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&GetReplicaResult{
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdGetReplica,
			Datatype:     0,
			Cas:          0,
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		ReplicaIdx:       opts.ReplicaIdx,
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// GetOneReplicaEx retrieves a document from a replica server.
func (agent *Agent) GetOneReplicaEx(opts GetOneReplicaOptions, cb GetReplicaExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetReplicaEx", opts.TraceContext)

	if opts.ReplicaIdx <= 0 {
		tracer.Finish()
		return nil, ErrInvalidReplica
	}

	return agent.getOneReplica(tracer, opts, func(resp *GetReplicaResult, err error) {
		tracer.Finish()
		cb(resp, err)
	})
}

// GetAnyReplicaEx retrieves a document from any replica or active server.
func (agent *Agent) GetAnyReplicaEx(opts GetAnyReplicaOptions, cb GetReplicaExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetAnyReplicaEx", opts.TraceContext)

	numReplicas := agent.NumReplicas()

	if numReplicas == 0 {
		tracer.Finish()
		return nil, ErrInvalidReplica
	}

	var resultLock sync.Mutex
	var firstResult *GetReplicaResult

	op := new(multiPendingOp)
	op.isIdempotent = true
	expected := uint32(numReplicas) + 1

	opHandledLocked := func() {
		completed := op.IncrementCompletedOps()
		if expected-completed == 0 {
			if firstResult == nil {
				tracer.Finish()
				cb(nil, ErrNoReplicas)
				return
			}

			tracer.Finish()
			cb(firstResult, nil)
		}
	}

	handler := func(resp *GetReplicaResult, err error) {
		resultLock.Lock()

		if err != nil {
			opHandledLocked()
			resultLock.Unlock()
			return
		}

		if firstResult == nil {
			newReplica := *resp
			firstResult = &newReplica
		}

		// Mark this op as completed
		opHandledLocked()

		// Try to cancel every other operation so we can
		// return as soon as possible to the user (and close
		// any open tracing spans)
		for _, op := range op.ops {
			if op.Cancel() {
				opHandledLocked()
			}
		}

		resultLock.Unlock()
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	getOp, err := agent.GetEx(GetOptions{
		ScopeName:      opts.ScopeName,
		RetryStrategy:  opts.RetryStrategy,
		CollectionName: opts.CollectionName,
		TraceContext:   opts.TraceContext,
		Key:            opts.Key,
	}, func(result *GetResult, err error) {
		if err != nil {
			handler(nil, err)
			return
		}
		handler(&GetReplicaResult{
			Flags:    result.Flags,
			Value:    result.Value,
			Cas:      result.Cas,
			Datatype: result.Datatype,
			IsActive: true,
		}, nil)
	})

	resultLock.Lock()
	if err == nil {
		op.ops = append(op.ops, getOp)
	} else {
		opHandledLocked()
	}
	resultLock.Unlock()

	// Dispatch a getReplica for each replica server
	for repIdx := 1; repIdx <= numReplicas; repIdx++ {
		subOp, err := agent.getOneReplica(tracer, GetOneReplicaOptions{
			Key:            opts.Key,
			ReplicaIdx:     repIdx,
			CollectionName: opts.CollectionName,
			ScopeName:      opts.ScopeName,
			CollectionID:   opts.CollectionID,
			RetryStrategy:  opts.RetryStrategy,
		}, handler)

		resultLock.Lock()

		if err != nil {
			opHandledLocked()
			resultLock.Unlock()
			continue
		}

		op.ops = append(op.ops, subOp)
		resultLock.Unlock()
	}

	return op, nil
}

// TouchOptions encapsulates the parameters for a TouchEx operation.
type TouchOptions struct {
	Key            []byte
	Expiry         uint32
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// TouchResult encapsulates the result of a TouchEx operation.
type TouchResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// TouchExCallback is invoked upon completion of a TouchEx operation.
type TouchExCallback func(*TouchResult, error)

// TouchEx updates the expiry for a document.
func (agent *Agent) TouchEx(opts TouchOptions, cb TouchExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("TouchEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&TouchResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Expiry)

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       cmdTouch,
			Datatype:     0,
			Cas:          0,
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// UnlockOptions encapsulates the parameters for a UnlockEx operation.
type UnlockOptions struct {
	Key            []byte
	Cas            Cas
	CollectionName string
	ScopeName      string
	CollectionID   uint32
	RetryStrategy  RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// UnlockResult encapsulates the result of a UnlockEx operation.
type UnlockResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// UnlockExCallback is invoked upon completion of a UnlockEx operation.
type UnlockExCallback func(*UnlockResult, error)

// UnlockEx unlocks a locked document.
func (agent *Agent) UnlockEx(opts UnlockOptions, cb UnlockExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("UnlockEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&UnlockResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        reqMagic,
			Opcode:       cmdUnlockKey,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// DeleteOptions encapsulates the parameters for a DeleteEx operation.
type DeleteOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Cas                    Cas
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// DeleteResult encapsulates the result of a DeleteEx operation.
type DeleteResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// DeleteExCallback is invoked upon completion of a DeleteEx operation.
type DeleteExCallback func(*DeleteResult, error)

// DeleteEx removes a document.
func (agent *Agent) DeleteEx(opts DeleteOptions, cb DeleteExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("DeleteEx", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&DeleteResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if agent.durabilityLevelStatus == durabilityLevelStatusUnsupported {
			return nil, ErrEnhancedDurabilityUnsupported
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       cmdDelete,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       nil,
			Key:          opts.Key,
			Value:        nil,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

type storeOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Cas                    Cas
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// StoreResult encapsulates the result of a AddEx, SetEx or ReplaceEx operation.
type StoreResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// StoreExCallback is invoked upon completion of a AddEx, SetEx or ReplaceEx operation.
type StoreExCallback func(*StoreResult, error)

func (agent *Agent) storeEx(opName string, opcode commandCode, opts storeOptions, cb StoreExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace(opName, opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&StoreResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if agent.durabilityLevelStatus == durabilityLevelStatusUnsupported {
			return nil, ErrEnhancedDurabilityUnsupported
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], opts.Flags)
	binary.BigEndian.PutUint32(extraBuf[4:], opts.Expiry)
	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       opcode,
			Datatype:     opts.Datatype,
			Cas:          uint64(opts.Cas),
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        opts.Value,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// AddOptions encapsulates the parameters for a AddEx operation.
type AddOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// AddEx stores a document as long as it does not already exist.
func (agent *Agent) AddEx(opts AddOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.storeEx("AddEx", cmdAdd, storeOptions{
		Key:                    opts.Key,
		CollectionName:         opts.CollectionName,
		ScopeName:              opts.ScopeName,
		RetryStrategy:          opts.RetryStrategy,
		Value:                  opts.Value,
		Flags:                  opts.Flags,
		Datatype:               opts.Datatype,
		Cas:                    0,
		Expiry:                 opts.Expiry,
		TraceContext:           opts.TraceContext,
		DurabilityLevel:        opts.DurabilityLevel,
		DurabilityLevelTimeout: opts.DurabilityLevelTimeout,
		CollectionID:           opts.CollectionID,
	}, cb)
}

// SetOptions encapsulates the parameters for a SetEx operation.
type SetOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// SetEx stores a document.
func (agent *Agent) SetEx(opts SetOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.storeEx("SetEx", cmdSet, storeOptions{
		Key:                    opts.Key,
		CollectionName:         opts.CollectionName,
		ScopeName:              opts.ScopeName,
		RetryStrategy:          opts.RetryStrategy,
		Value:                  opts.Value,
		Flags:                  opts.Flags,
		Datatype:               opts.Datatype,
		Cas:                    0,
		Expiry:                 opts.Expiry,
		TraceContext:           opts.TraceContext,
		DurabilityLevel:        opts.DurabilityLevel,
		DurabilityLevelTimeout: opts.DurabilityLevelTimeout,
		CollectionID:           opts.CollectionID,
	}, cb)
}

// ReplaceOptions encapsulates the parameters for a ReplaceEx operation.
type ReplaceOptions struct {
	Key                    []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Value                  []byte
	Flags                  uint32
	Datatype               uint8
	Cas                    Cas
	Expiry                 uint32
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// ReplaceEx replaces the value of a Couchbase document with another value.
func (agent *Agent) ReplaceEx(opts ReplaceOptions, cb StoreExCallback) (PendingOp, error) {
	return agent.storeEx("ReplaceEx", cmdReplace, storeOptions{
		Key:                    opts.Key,
		CollectionName:         opts.CollectionName,
		ScopeName:              opts.ScopeName,
		RetryStrategy:          opts.RetryStrategy,
		Value:                  opts.Value,
		Flags:                  opts.Flags,
		Datatype:               opts.Datatype,
		Cas:                    opts.Cas,
		Expiry:                 opts.Expiry,
		TraceContext:           opts.TraceContext,
		DurabilityLevel:        opts.DurabilityLevel,
		DurabilityLevelTimeout: opts.DurabilityLevelTimeout,
		CollectionID:           opts.CollectionID,
	}, cb)
}

// AdjoinOptions encapsulates the parameters for a AppendEx or PrependEx operation.
type AdjoinOptions struct {
	Key                    []byte
	Value                  []byte
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Cas                    Cas
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// AdjoinResult encapsulates the result of a AppendEx or PrependEx operation.
type AdjoinResult struct {
	Cas           Cas
	MutationToken MutationToken
}

// AdjoinExCallback is invoked upon completion of a AppendEx or PrependEx operation.
type AdjoinExCallback func(*AdjoinResult, error)

func (agent *Agent) adjoinEx(opName string, opcode commandCode, opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace(opName, opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&AdjoinResult{
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if agent.durabilityLevelStatus == durabilityLevelStatusUnsupported {
			return nil, ErrEnhancedDurabilityUnsupported
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       opcode,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       nil,
			Key:          opts.Key,
			Value:        opts.Value,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// AppendEx appends some bytes to a document.
func (agent *Agent) AppendEx(opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	return agent.adjoinEx("AppendEx", cmdAppend, opts, cb)
}

// PrependEx prepends some bytes to a document.
func (agent *Agent) PrependEx(opts AdjoinOptions, cb AdjoinExCallback) (PendingOp, error) {
	return agent.adjoinEx("PrependEx", cmdPrepend, opts, cb)
}

// CounterOptions encapsulates the parameters for a IncrementEx or DecrementEx operation.
type CounterOptions struct {
	Key                    []byte
	Delta                  uint64
	Initial                uint64
	Expiry                 uint32
	CollectionName         string
	ScopeName              string
	RetryStrategy          RetryStrategy
	Cas                    Cas
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
	CollectionID           uint32

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// CounterResult encapsulates the result of a IncrementEx or DecrementEx operation.
type CounterResult struct {
	Value         uint64
	Cas           Cas
	MutationToken MutationToken
}

// CounterExCallback is invoked upon completion of a IncrementEx or DecrementEx operation.
type CounterExCallback func(*CounterResult, error)

func (agent *Agent) counterEx(opName string, opcode commandCode, opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace(opName, opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Value) != 8 {
			tracer.Finish()
			cb(nil, ErrProtocol)
			return
		}
		intVal := binary.BigEndian.Uint64(resp.Value)

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbID = req.Vbucket
			mutToken.VbUUID = VbUUID(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		tracer.Finish()
		cb(&CounterResult{
			Value:         intVal,
			Cas:           Cas(resp.Cas),
			MutationToken: mutToken,
		}, nil)
	}

	// You cannot have an expiry when you do not want to create the document.
	if opts.Initial == uint64(0xFFFFFFFFFFFFFFFF) && opts.Expiry != 0 {
		return nil, ErrInvalidArgs
	}

	magic := reqMagic
	var flexibleFrameExtras *memdFrameExtras
	if opts.DurabilityLevel > 0 {
		if agent.durabilityLevelStatus == durabilityLevelStatusUnsupported {
			return nil, ErrEnhancedDurabilityUnsupported
		}
		flexibleFrameExtras = &memdFrameExtras{}
		flexibleFrameExtras.DurabilityLevel = opts.DurabilityLevel
		flexibleFrameExtras.DurabilityLevelTimeout = opts.DurabilityLevelTimeout
		magic = altReqMagic
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], opts.Delta)
	if opts.Initial != uint64(0xFFFFFFFFFFFFFFFF) {
		binary.BigEndian.PutUint64(extraBuf[8:], opts.Initial)
		binary.BigEndian.PutUint32(extraBuf[16:], opts.Expiry)
	} else {
		binary.BigEndian.PutUint64(extraBuf[8:], 0x0000000000000000)
		binary.BigEndian.PutUint32(extraBuf[16:], 0xFFFFFFFF)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:        magic,
			Opcode:       opcode,
			Datatype:     0,
			Cas:          uint64(opts.Cas),
			Extras:       extraBuf,
			Key:          opts.Key,
			Value:        nil,
			FrameExtras:  flexibleFrameExtras,
			CollectionID: opts.CollectionID,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		CollectionName:   opts.CollectionName,
		ScopeName:        opts.ScopeName,
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// IncrementEx increments the unsigned integer value in a document.
func (agent *Agent) IncrementEx(opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	return agent.counterEx("IncrementEx", cmdIncrement, opts, cb)
}

// DecrementEx decrements the unsigned integer value in a document.
func (agent *Agent) DecrementEx(opts CounterOptions, cb CounterExCallback) (PendingOp, error) {
	return agent.counterEx("DecrementEx", cmdDecrement, opts, cb)
}

// GetRandomOptions encapsulates the parameters for a GetRandomEx operation.
type GetRandomOptions struct {
	RetryStrategy RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetRandomResult encapsulates the result of a GetRandomEx operation.
type GetRandomResult struct {
	Key      []byte
	Value    []byte
	Flags    uint32
	Datatype uint8
	Cas      Cas
}

// GetRandomExCallback is invoked upon completion of a GetRandomEx operation.
type GetRandomExCallback func(*GetRandomResult, error)

// GetRandomEx retrieves the key and value of a random document stored within Couchbase Server.
func (agent *Agent) GetRandomEx(opts GetRandomOptions, cb GetRandomExCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetRandomEx", opts.TraceContext)

	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(nil, err)
			return
		}

		if len(resp.Extras) != 4 {
			tracer.Finish()
			cb(nil, ErrProtocol)
			return
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		tracer.Finish()
		cb(&GetRandomResult{
			Key:      resp.Key,
			Value:    resp.Value,
			Flags:    flags,
			Cas:      Cas(resp.Cas),
			Datatype: resp.Datatype,
		}, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetRandom,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback:         handler,
		RootTraceContext: tracer.RootContext(),
		RetryStrategy:    opts.RetryStrategy,
	}

	return agent.dispatchOp(req)
}

// SingleServerStats represents the stats returned from a single server.
type SingleServerStats struct {
	Stats map[string]string
	Error error
}

// StatsTarget is used for providing a specific target to the StatsEx operation.
type StatsTarget interface {
}

// VBucketIDStatsTarget indicates that a specific vbucket should be targeted by the StatsEx operation.
type VBucketIDStatsTarget struct {
	VbID uint16
}

// StatsOptions encapsulates the parameters for a StatsEx operation.
type StatsOptions struct {
	Key string
	// Target indicates that something specific should be targeted by the operation. If left nil
	// then the stats command will be sent to all servers.
	Target        StatsTarget
	RetryStrategy RetryStrategy

	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// StatsResult encapsulates the result of a StatsEx operation.
type StatsResult struct {
	Servers map[string]SingleServerStats
}

// StatsExCallback is invoked upon completion of a StatsEx operation.
type StatsExCallback func(*StatsResult, error)

// StatsEx retrieves statistics information from the server.  Note that as this
// function is an aggregator across numerous servers, there are no guarantees
// about the consistency of the results.  Occasionally, some nodes may not be
// represented in the results, or there may be conflicting information between
// multiple nodes (a vbucket active on two separate nodes at once).
func (agent *Agent) StatsEx(opts StatsOptions, cb StatsExCallback) (CancellablePendingOp, error) {
	tracer := agent.createOpTrace("StatsEx", opts.TraceContext)

	config := agent.routingInfo.Get()
	if config == nil {
		tracer.Finish()
		return nil, ErrShutdown
	}

	stats := make(map[string]SingleServerStats)
	var statsLock sync.Mutex

	op := new(multiPendingOp)
	op.isIdempotent = true
	var expected uint32

	pipelines := make([]*memdPipeline, 0)

	switch target := opts.Target.(type) {
	case nil:
		expected = uint32(config.clientMux.NumPipelines())

		for i := 0; i < config.clientMux.NumPipelines(); i++ {
			pipelines = append(pipelines, config.clientMux.GetPipeline(i))
		}
	case VBucketIDStatsTarget:
		expected = 1

		srvIdx, err := config.vbMap.NodeByVbucket(target.VbID, 0)
		if err != nil {
			return nil, err
		}

		pipelines = append(pipelines, config.clientMux.GetPipeline(srvIdx))
	default:
		return nil, ErrUnsupportedStatsTarget
	}

	opHandledLocked := func() {
		completed := op.IncrementCompletedOps()
		if expected-completed == 0 {
			tracer.Finish()
			cb(&StatsResult{
				Servers: stats,
			}, nil)
		}
	}

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		serverAddress := resp.sourceAddr

		statsLock.Lock()
		defer statsLock.Unlock()

		// Fetch the specific stats key for this server.  Creating a new entry
		// for the server if we did not previously have one.
		curStats, ok := stats[serverAddress]
		if !ok {
			stats[serverAddress] = SingleServerStats{
				Stats: make(map[string]string),
			}
			curStats = stats[serverAddress]
		}

		if err != nil {
			// Store the first (and hopefully only) error into the Error field of this
			// server's stats entry.
			if curStats.Error == nil {
				curStats.Error = err
			} else {
				logDebugf("Got additional error for stats: %s: %v", serverAddress, err)
			}

			// When an error occurs, we need to cancel our persistent op.  However, because
			// a previous error may already have cancelled this and then raced, we should
			// ensure only a single completion is counted.
			if req.Cancel() {
				opHandledLocked()
			}

			return
		}

		// Check if the key length is zero.  This indicates that we have reached
		// the ending of the stats listing by this server.
		if len(resp.Key) == 0 {
			// As this is a persistent request, we must manually cancel it to remove
			// it from the pending ops list.  To ensure we do not race multiple cancels,
			// we only handle it as completed the one time cancellation succeeds.
			if req.Cancel() {
				opHandledLocked()
			}

			return
		}

		// Add the stat for this server to the list of stats.
		curStats.Stats[string(resp.Key)] = string(resp.Value)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	for _, pipeline := range pipelines {
		serverAddress := pipeline.Address()

		req := &memdQRequest{
			memdPacket: memdPacket{
				Magic:    reqMagic,
				Opcode:   cmdStat,
				Datatype: 0,
				Cas:      0,
				Key:      []byte(opts.Key),
				Value:    nil,
			},
			Persistent:       true,
			Callback:         handler,
			RootTraceContext: tracer.RootContext(),
			RetryStrategy:    opts.RetryStrategy,
		}

		curOp, err := agent.dispatchOpToAddress(req, serverAddress)
		if err != nil {
			statsLock.Lock()
			stats[serverAddress] = SingleServerStats{
				Error: err,
			}
			opHandledLocked()
			statsLock.Unlock()

			continue
		}

		op.ops = append(op.ops, curOp)
	}

	return op, nil
}
