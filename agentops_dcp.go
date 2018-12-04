package gocbcore

import (
	"encoding/binary"
	"encoding/json"
)

// SnapshotState represents the state of a particular cluster snapshot.
type SnapshotState uint32

// HasInMemory returns whether this snapshot is available in memory.
func (s SnapshotState) HasInMemory() bool {
	return uint32(s)&1 != 0
}

// HasOnDisk returns whether this snapshot is available on disk.
func (s SnapshotState) HasOnDisk() bool {
	return uint32(s)&2 != 0
}

// FailoverEntry represents a single entry in the server fail-over log.
type FailoverEntry struct {
	VbUuid VbUuid
	SeqNo  SeqNo
}

// StreamObserver provides an interface to receive events from a running DCP stream.
type StreamObserver interface {
	SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, snapshotType SnapshotState)
	Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, key, value []byte)
	Deletion(seqNo, revNo, cas uint64, datatype uint8, vbId uint16, key, value []byte)
	Expiration(seqNo, revNo, cas uint64, vbId uint16, key []byte)
	End(vbId uint16, err error)
}

type CollectionStreamObserver interface {
	SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, snapshotType SnapshotState)
	Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, collectionId uint32, key, value []byte)
	Deletion(seqNo, revNo, cas uint64, datatype uint8, vbId uint16, collectionId uint32, key, value []byte)
	Expiration(seqNo, revNo, cas uint64, vbId uint16, collectionId uint32, key []byte)
	End(vbId uint16, err error)
	CreateCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32, ttl uint32, key []byte)
	DeleteCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, collectionId uint32)
	// FlushCollection(seqNo uint64, version uint8, vbId uint16, manifest_uid uint64, collection_id uint32)	// Not yet existing
	CreateScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32, key []byte)
	DeleteScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32)
	ModifyCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, collectionId uint32, ttl uint32)
}

type CollectionStreamFilter struct {
	ManifestUid string   `json:"uid,omitempty"`
	Collections []string `json:"collections,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	StreamId    uint16   `json:"sid,omitempty"`
}

type collectionStreamFilter struct {
	ManifestUid string   `json:"uid,omitempty"`
	Collections []string `json:"collections,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	StreamId    string   `json:"sid,omitempty"`
}

// OpenStreamCallback is invoked with the results of `OpenStream` operations.
type OpenStreamCallback func([]FailoverEntry, error)

// CloseStreamCallback is invoked with the results of `CloseStream` operations.
type CloseStreamCallback func(error)

// GetFailoverLogCallback is invoked with the results of `GetFailoverLog` operations.
type GetFailoverLogCallback func([]FailoverEntry, error)

// VbSeqNoEntry represents a single GetVbucketSeqnos sequence number entry.
type VbSeqNoEntry struct {
	VbId  uint16
	SeqNo SeqNo
}

// GetVBucketSeqnosCallback is invoked with the results of `GetVBucketSeqnos` operations.
type GetVBucketSeqnosCallback func([]VbSeqNoEntry, error)

// OpenStream opens a DCP stream for a particular VBucket.
func (agent *Agent) OpenCollectionStream(vbId uint16, flags DcpStreamAddFlag, vbUuid VbUuid, startSeqNo,
	endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler CollectionStreamObserver, filter *CollectionStreamFilter, cb OpenStreamCallback) (PendingOp, error) {
	var req *memdQRequest
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if resp != nil && resp.Magic == resMagic {
			// This is the response to the open stream request.
			if err != nil {
				req.Cancel()

				// All client errors are handled by the StreamObserver
				cb(nil, err)
				return
			}

			numEntries := len(resp.Value) / 16
			entries := make([]FailoverEntry, numEntries)
			for i := 0; i < numEntries; i++ {
				entries[i] = FailoverEntry{
					VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
					SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
				}
			}

			cb(entries, nil)
			return
		}

		if err != nil {
			req.Cancel()
			evtHandler.End(vbId, err)
			return
		}

		// This is one of the stream events
		switch resp.Opcode {
		case cmdDcpSnapshotMarker:
			vbId := uint16(resp.Vbucket)
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, vbId, SnapshotState(snapshotType))
		case cmdDcpMutation:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			flags := binary.BigEndian.Uint32(resp.Extras[16:])
			expiry := binary.BigEndian.Uint32(resp.Extras[20:])
			lockTime := binary.BigEndian.Uint32(resp.Extras[24:])
			collectionId, n := decodeleb128_32(resp.Key)
			key := resp.Key[n:]
			evtHandler.Mutation(seqNo, revNo, flags, expiry, lockTime, resp.Cas, resp.Datatype, vbId, collectionId, key, resp.Value)
		case cmdDcpDeletion:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			collectionId, n := decodeleb128_32(resp.Key)
			key := resp.Key[n:]
			evtHandler.Deletion(seqNo, revNo, resp.Cas, resp.Datatype, vbId, collectionId, key, resp.Value)
		case cmdDcpExpiration:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			collectionId, n := decodeleb128_32(resp.Key)
			key := resp.Key[n:]
			evtHandler.Expiration(seqNo, revNo, resp.Cas, vbId, collectionId, key)
		case cmdDcpEvent:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			eventCode := StreamEventCode(binary.BigEndian.Uint32(resp.Extras[8:]))
			version := resp.Extras[12]

			switch eventCode {
			case StreamEventCollectionCreate:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				collectionId := binary.BigEndian.Uint32(resp.Value[12:])
				var ttl uint32
				if version == 1 {
					ttl = binary.BigEndian.Uint32(resp.Value[16:])
				}
				evtHandler.CreateCollection(seqNo, version, vbId, manifestUid, scopeId, collectionId, ttl, resp.Key)
			case StreamEventCollectionDelete:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				collectionId := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.DeleteCollection(seqNo, version, vbId, manifestUid, collectionId)
			case StreamEventCollectionFlush:
				// This isn't yet in existence but proposed
			case StreamEventScopeCreate:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.CreateScope(seqNo, version, vbId, manifestUid, scopeId, resp.Key)
			case StreamEventScopeDelete:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.DeleteScope(seqNo, version, vbId, manifestUid, scopeId)
			case StreamEventCollectionChanged:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				collectionId := binary.BigEndian.Uint32(resp.Value[8:])
				ttl := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.ModifyCollection(seqNo, version, vbId, manifestUid, collectionId, ttl)
			}
		case cmdDcpStreamEnd:
			vbId := uint16(resp.Vbucket)
			code := streamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			evtHandler.End(vbId, getStreamEndError(code))
			req.Cancel()
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(flags))
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(endSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(vbUuid))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(snapStartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(snapEndSeqNo))

	var val []byte
	val = nil
	if filter != nil {
		// convertedFilter := collectionStreamFilter{}
		// for _, cid := range filter.Collections {
		// 	convertedFilter.Collections = append(convertedFilter.Collections, fmt.Sprintf("%x", cid))
		// }
		// convertedFilter.Scope = fmt.Sprintf("%d", filter.Scope)
		// convertedFilter.ManifestUid = fmt.Sprintf("%d", filter.ManifestUid)
		// convertedFilter.StreamId = fmt.Sprintf("%d", filter.StreamId)
		var err error
		val, err = json.Marshal(filter)
		if err != nil {
			return nil, err
		}
	}

	req = &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpStreamReq,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    val,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return agent.dispatchOp(req)
}

// OpenStream opens a DCP stream for a particular VBucket.
func (agent *Agent) OpenStream(vbId uint16, flags DcpStreamAddFlag, vbUuid VbUuid, startSeqNo, endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler StreamObserver, cb OpenStreamCallback) (PendingOp, error) {
	var req *memdQRequest
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if resp != nil && resp.Magic == resMagic {
			// This is the response to the open stream request.
			if err != nil {
				req.Cancel()

				// All client errors are handled by the StreamObserver
				cb(nil, err)
				return
			}

			numEntries := len(resp.Value) / 16
			entries := make([]FailoverEntry, numEntries)
			for i := 0; i < numEntries; i++ {
				entries[i] = FailoverEntry{
					VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
					SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
				}
			}

			cb(entries, nil)
			return
		}

		if err != nil {
			req.Cancel()
			evtHandler.End(vbId, err)
			return
		}

		// This is one of the stream events
		switch resp.Opcode {
		case cmdDcpSnapshotMarker:
			vbId := uint16(resp.Vbucket)
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, vbId, SnapshotState(snapshotType))
		case cmdDcpMutation:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			flags := binary.BigEndian.Uint32(resp.Extras[16:])
			expiry := binary.BigEndian.Uint32(resp.Extras[20:])
			lockTime := binary.BigEndian.Uint32(resp.Extras[24:])
			evtHandler.Mutation(seqNo, revNo, flags, expiry, lockTime, resp.Cas, resp.Datatype, vbId, resp.Key, resp.Value)
		case cmdDcpDeletion:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			evtHandler.Deletion(seqNo, revNo, resp.Cas, resp.Datatype, vbId, resp.Key, resp.Value)
		case cmdDcpExpiration:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			evtHandler.Expiration(seqNo, revNo, resp.Cas, vbId, resp.Key)
		case cmdDcpStreamEnd:
			vbId := uint16(resp.Vbucket)
			code := streamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			evtHandler.End(vbId, getStreamEndError(code))
			req.Cancel()
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(flags))
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(endSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(vbUuid))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(snapStartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(snapEndSeqNo))

	req = &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpStreamReq,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return agent.dispatchOp(req)
}

// CloseStream shuts down an open stream for the specified VBucket.
func (agent *Agent) CloseStream(vbId uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpCloseStream,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: false,
	}
	return agent.dispatchOp(req)
}

// GetFailoverLog retrieves the fail-over log for a particular VBucket.  This is used
// to resume an interrupted stream after a node fail-over has occurred.
func (agent *Agent) GetFailoverLog(vbId uint16, cb GetFailoverLogCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		numEntries := len(resp.Value) / 16
		entries := make([]FailoverEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = FailoverEntry{
				VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
				SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
			}
		}
		cb(entries, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpGetFailoverLog,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: false,
	}
	return agent.dispatchOp(req)
}

// GetVbucketSeqnos returns the last checkpoint for a particular VBucket.  This is useful
// for starting a DCP stream from wherever the server currently is.
func (agent *Agent) GetVbucketSeqnos(serverIdx int, state VbucketState, cb GetVBucketSeqnosCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		var vbs []VbSeqNoEntry

		numVbs := len(resp.Value) / 10
		for i := 0; i < numVbs; i++ {
			vbs = append(vbs, VbSeqNoEntry{
				VbId:  binary.BigEndian.Uint16(resp.Value[i*10:]),
				SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*10+2:])),
			})
		}

		cb(vbs, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(state))

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetAllVBSeqnos,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  0,
		},
		Callback:   handler,
		ReplicaIdx: -serverIdx,
		Persistent: false,
	}

	return agent.dispatchOp(req)
}
