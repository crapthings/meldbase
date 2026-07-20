package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// CollectionAccessManifestVersion is the only supported collection-access
// manifest grammar. A versioned manifest is intentionally data-only so tools
// and agents can generate and validate the same server configuration.
const CollectionAccessManifestVersion = 1

// CollectionAccessManifestSchemaURL is the canonical editor and tooling schema
// for the current manifest grammar. It is optional metadata, but when present
// the strict parser accepts only this exact versioned URL.
const CollectionAccessManifestSchemaURL = "https://crapthings.github.io/meldbase/schemas/collection-access-manifest-v1.schema.json"

// CollectionAccessManifest is a strict, portable declaration of the generic
// client data API. It contains no executable authorization callbacks: complex
// membership or role rules still use an application Authorizer or Worker policy
// resolver, both of which return the same server-enforced policy types.
type CollectionAccessManifest struct {
	SchemaURL      string             `json:"$schema,omitempty"`
	Version        int                `json:"version"`
	WorkspaceField string             `json:"workspaceField"`
	Collections    []CollectionAccess `json:"collections"`
}

// WorkspaceAuthorizerConfig validates the manifest and returns the equivalent
// built-in authorizer configuration.
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
	}
	if _, err := NewWorkspaceAuthorizer(config); err != nil {
		return WorkspaceAuthorizerConfig{}, err
	}
	return config, nil
}

// ParseCollectionAccessManifestJSON rejects unknown fields, trailing values,
// unsupported versions, and invalid collection declarations before a server is
// started.
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
