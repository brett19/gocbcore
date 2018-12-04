package gocbcore

import (
	"encoding/json"
	"strconv"
)

type CollectionManifestCollection struct {
	UID  uint32
	Name string
}

func (item *CollectionManifestCollection) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID  string `json:"uid"`
		Name string `json:"name"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 32)
	if err != nil {
		return err
	}

	item.UID = uint32(decUID)
	item.Name = decData.Name
	return nil
}

type CollectionManifestScope struct {
	UID         uint32
	Name        string
	Collections []CollectionManifestCollection
}

func (item *CollectionManifestScope) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID         string                         `json:"uid"`
		Name        string                         `json:"name"`
		Collections []CollectionManifestCollection `json:"collections"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 32)
	if err != nil {
		return err
	}

	item.UID = uint32(decUID)
	item.Name = decData.Name
	item.Collections = decData.Collections
	return nil
}

type CollectionManifest struct {
	UID    uint64
	Scopes []CollectionManifestScope
}

func (item *CollectionManifest) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID    string                    `json:"uid"`
		Scopes []CollectionManifestScope `json:"scopes"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 64)
	if err != nil {
		return err
	}

	item.UID = decUID
	item.Scopes = decData.Scopes
	return nil
}

type CollectionManifestCallback func(manifest []byte, err error)

func (agent *Agent) GetCollectionManifest(cb CollectionManifestCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		// log.Printf("Got Manifest %+v %+v %+v", err, resp, req)
		if err != nil {
			cb(nil, err)
			return
		}

		cb(resp.Value, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdCollectionsGetManifest,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback: handler,
	}
	// log.Printf("Requesting manifest %+v", req)
	return agent.dispatchOp(req)
}
