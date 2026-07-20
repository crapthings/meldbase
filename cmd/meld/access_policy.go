package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/crapthings/meldbase/core"
	meldserver "github.com/crapthings/meldbase/server"
)

// runAccessPolicy validates or explains the same strict manifest that meld
// serve accepts. It never opens a database, validates a JWT, or issues a
// credential; explain is a static policy simulation for review and tests.
func runAccessPolicy(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: meld access-policy <validate|explain> [flags]")
	}
	switch args[0] {
	case "validate":
		flags := flag.NewFlagSet("access-policy validate", flag.ContinueOnError)
		flags.SetOutput(stderr)
		file := flags.String("file", "", "collection access manifest JSON file")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		manifest, err := readCollectionAccessManifest(*file)
		if err != nil {
			return err
		}
		return writeCollectionAccessJSON(stdout, manifest)
	case "explain":
		flags := flag.NewFlagSet("access-policy explain", flag.ContinueOnError)
		flags.SetOutput(stderr)
		file := flags.String("file", "", "collection access manifest JSON file")
		subject := flags.String("subject", "", "simulated authenticated JWT sub")
		workspace := flags.String("workspace", "", "simulated authenticated active workspace")
		collection := flags.String("collection", "", "declared collection to explain")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *subject == "" || *workspace == "" || *collection == "" {
			return errors.New("--subject, --workspace, and --collection are required")
		}
		manifest, err := readCollectionAccessManifest(*file)
		if err != nil {
			return err
		}
		for _, rule := range manifest.Collections {
			if rule.Collection == *collection {
				return explainCollectionAccess(stdout, manifest, rule, meldserver.Principal{Subject: *subject, Tenant: *workspace})
			}
		}
		return fmt.Errorf("collection %q is not declared by the manifest", *collection)
	default:
		return fmt.Errorf("unknown access-policy command %q", args[0])
	}
}

func readCollectionAccessManifest(path string) (meldserver.CollectionAccessManifest, error) {
	if path == "" {
		return meldserver.CollectionAccessManifest{}, errors.New("--file is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return meldserver.CollectionAccessManifest{}, fmt.Errorf("read collection access manifest: %w", err)
	}
	manifest, err := meldserver.ParseCollectionAccessManifestJSON(data)
	if err != nil {
		return meldserver.CollectionAccessManifest{}, fmt.Errorf("parse collection access manifest: %w", err)
	}
	return manifest, nil
}

func writeCollectionAccessJSON(stdout io.Writer, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, string(encoded))
	return err
}

type collectionAccessExplanation struct {
	Version    int                                  `json:"version"`
	Collection string                               `json:"collection"`
	Mode       meldserver.CollectionAccessMode      `json:"mode"`
	Principal  collectionAccessExplanationPrincipal `json:"principal"`
	Query      collectionAccessQueryExplanation     `json:"query"`
	Insert     collectionAccessInsertExplanation    `json:"insert"`
	Update     collectionAccessMutationExplanation  `json:"update"`
	Delete     collectionAccessMutationExplanation  `json:"delete"`
}

type collectionAccessExplanationPrincipal struct {
	Subject   string `json:"subject"`
	Workspace string `json:"workspace"`
}

type collectionAccessQueryExplanation struct {
	Allowed      bool            `json:"allowed"`
	Constraint   json.RawMessage `json:"constraint,omitempty"`
	MaxResults   int             `json:"maxResults,omitempty"`
	QueryPaths   string          `json:"queryPaths,omitempty"`
	ResultFields string          `json:"resultFields,omitempty"`
}

type collectionAccessInsertExplanation struct {
	Allowed      bool              `json:"allowed"`
	InputFields  string            `json:"inputFields,omitempty"`
	ResultFields string            `json:"resultFields,omitempty"`
	ServerFields map[string]string `json:"serverFields,omitempty"`
}

type collectionAccessMutationExplanation struct {
	Allowed           bool     `json:"allowed"`
	QueryPaths        string   `json:"queryPaths,omitempty"`
	UpdatePaths       string   `json:"updatePaths,omitempty"`
	DeniedUpdatePaths []string `json:"deniedUpdatePaths,omitempty"`
	MaxAffected       int      `json:"maxAffected,omitempty"`
}

func explainCollectionAccess(stdout io.Writer, manifest meldserver.CollectionAccessManifest, rule meldserver.CollectionAccess, principal meldserver.Principal) error {
	config, err := manifest.WorkspaceAuthorizerConfig()
	if err != nil {
		return err
	}
	authorizer, err := meldserver.NewWorkspaceAuthorizer(config)
	if err != nil {
		return err
	}
	query, err := meldbase.CompileQuery(meldbase.Filter{}, meldbase.QueryOptions{})
	if err != nil {
		return err
	}
	explanation := collectionAccessExplanation{
		Version: 1, Collection: rule.Collection, Mode: rule.Mode,
		Principal: collectionAccessExplanationPrincipal{Subject: principal.Subject, Workspace: principal.Tenant},
	}
	if policy, policyErr := authorizer.AuthorizeQuery(context.Background(), principal, rule.Collection, query); policyErr == nil {
		constraint, marshalErr := meldbase.MarshalQuerySpecJSON(*policy.Constraint)
		if marshalErr != nil {
			return marshalErr
		}
		explanation.Query = collectionAccessQueryExplanation{
			Allowed: true, Constraint: constraint, MaxResults: policy.MaxResults,
			QueryPaths:   describePolicyFields(policy.AllowAllQueryPaths, policy.AllowedQueryPaths),
			ResultFields: describePolicyFields(policy.AllowAllResultFields, policy.AllowedResultFields),
		}
	} else if !errors.Is(policyErr, meldserver.ErrForbidden) {
		return policyErr
	}
	if policy, policyErr := authorizer.AuthorizeInsert(context.Background(), principal, rule.Collection, meldbase.Document{}); policyErr == nil {
		explanation.Insert.Allowed = true
		explanation.Insert.InputFields = describePolicyFields(policy.AllowAllInputFields, policy.AllowedInputFields)
		explanation.Insert.ResultFields = describePolicyFields(policy.AllowAllResultFields, policy.AllowedResultFields)
		explanation.Insert.ServerFields = stringPolicyFields(policy.SetFields)
	} else if !errors.Is(policyErr, meldserver.ErrForbidden) {
		return policyErr
	}
	if policy, policyErr := authorizer.AuthorizeUpdate(context.Background(), principal, rule.Collection, query, meldbase.MutationSpec{}); policyErr == nil {
		explanation.Update.Allowed = true
		explanation.Update.QueryPaths = describePolicyFields(policy.AllowAllQueryPaths, policy.AllowedQueryPaths)
		explanation.Update.UpdatePaths = describePolicyFields(policy.AllowAllUpdatePaths, policy.AllowedUpdatePaths)
		explanation.Update.DeniedUpdatePaths = sortedPolicyPaths(policy.DeniedUpdatePaths)
		explanation.Update.MaxAffected = policy.MaxAffected
	} else if !errors.Is(policyErr, meldserver.ErrForbidden) {
		return policyErr
	}
	if policy, policyErr := authorizer.AuthorizeDelete(context.Background(), principal, rule.Collection, query); policyErr == nil {
		explanation.Delete.Allowed = true
		explanation.Delete.MaxAffected = policy.MaxAffected
	} else if !errors.Is(policyErr, meldserver.ErrForbidden) {
		return policyErr
	}
	return writeCollectionAccessJSON(stdout, explanation)
}

func describePolicyFields(all bool, fields map[string]struct{}) string {
	if all {
		return "*"
	}
	return fmt.Sprintf("%v", sortedPolicyPaths(fields))
}

func sortedPolicyPaths(fields map[string]struct{}) []string {
	paths := make([]string, 0, len(fields))
	for path := range fields {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func stringPolicyFields(fields meldbase.Document) map[string]string {
	result := make(map[string]string, len(fields))
	for field, value := range fields {
		if text, ok := value.StringValue(); ok {
			result[field] = text
		}
	}
	return result
}
