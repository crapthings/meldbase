package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// CollectionAccessManifestVersion identifies the supported collection access manifest format.
//
// Manifests contain only data so tools and agents can generate and validate
// the same server configuration.
const CollectionAccessManifestVersion = 1

// CollectionAccessManifestSchemaURL identifies the schema used to validate collection access manifests.
//
// It is optional metadata, but when present the strict parser accepts only
// this exact versioned URL.
const CollectionAccessManifestSchemaURL = "https://crapthings.github.io/meldbase/schemas/collection-access-manifest-v1.schema.json"

// CollectionAccessManifest declares collection access rules without executable authorization callbacks.
//
// Complex
// membership or role rules still use an application Authorizer or Worker policy
// resolver, both of which return the same server-enforced policy types.
type CollectionAccessManifest struct {
	SchemaURL      string             `json:"$schema,omitempty"`
	Version        int                `json:"version"`
	WorkspaceField string             `json:"workspaceField"`
	Collections    []CollectionAccess `json:"collections"`
	RPCMethods     []string           `json:"rpcMethods,omitempty"`
}

// WorkspaceAuthorizerConfig validates the manifest and converts it into built-in access settings.
func (manifest CollectionAccessManifest) WorkspaceAuthorizerConfig() (WorkspaceAuthorizerConfig, error) {
	if manifest.SchemaURL != "" && manifest.SchemaURL != CollectionAccessManifestSchemaURL {
		return WorkspaceAuthorizerConfig{}, errors.New("collection access manifest schema URL is unsupported")
	}
	if manifest.Version != CollectionAccessManifestVersion {
		return WorkspaceAuthorizerConfig{}, errors.New("collection access manifest version is unsupported")
	}
	config := WorkspaceAuthorizerConfig{
		CollectionAccess: manifest.Collections,
		WorkspaceField:   manifest.WorkspaceField,
		RPCMethods:       manifest.RPCMethods,
	}
	if _, err := NewWorkspaceAuthorizer(config); err != nil {
		return WorkspaceAuthorizerConfig{}, err
	}
	return config, nil
}

// ParseCollectionAccessManifestJSON decodes and validates a collection access manifest.
//
// It rejects unknown fields, trailing values, unsupported versions, and
// invalid collection declarations.
func ParseCollectionAccessManifestJSON(data []byte) (CollectionAccessManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest CollectionAccessManifest
	if err := decoder.Decode(&manifest); err != nil {
		return CollectionAccessManifest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return CollectionAccessManifest{}, errors.New("collection access manifest contains trailing JSON")
		}
		return CollectionAccessManifest{}, err
	}
	if _, err := manifest.WorkspaceAuthorizerConfig(); err != nil {
		return CollectionAccessManifest{}, err
	}
	return manifest, nil
}
