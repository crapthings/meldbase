package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/crapthings/meldbase"
)

// insert, mutate, and query implement the generic data API. Their only
// authorization surface is the server-owned policy types; they never accept a
// client-selected workspace or projection.
func (h *Handler) insert(w http.ResponseWriter, r *http.Request) {
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	var envelope struct {
		Version  int             `json:"version"`
		Document json.RawMessage `json:"document"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Document) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_document_envelope")
		return
	}
	document, err := meldbase.UnmarshalWireInputDocument(envelope.Document, h.config.QueryLimits)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_document")
		return
	}
	policy, err := h.config.Authorizer.AuthorizeInsert(r.Context(), actor, r.PathValue("collection"), document.Clone())
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	policy = freezeInsertPolicy(policy)
	if !policy.AllowAllInputFields {
		for field := range document {
			if field == "_id" {
				continue
			}
			if _, allowed := policy.AllowedInputFields[field]; !allowed {
				writeError(w, http.StatusForbidden, "forbidden_field")
				return
			}
		}
	}
	for field, value := range policy.SetFields {
		if field == "_id" {
			writeError(w, http.StatusInternalServerError, "invalid_policy")
			return
		}
		document[field] = value.Clone()
	}
	id, err := h.config.DB.Collection(r.PathValue("collection")).InsertOne(r.Context(), document)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	inserted, err := h.config.DB.Collection(r.PathValue("collection")).FindOne(r.Context(), meldbase.Filter{"_id": id})
	if err != nil {
		writeEngineError(w, err)
		return
	}
	raw, err := meldbase.MarshalWireDocument(projectInsert(inserted, policy))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"version": 1, "document": json.RawMessage(raw)})
}

func (h *Handler) mutate(w http.ResponseWriter, r *http.Request) {
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	var envelope struct {
		Version int             `json:"version"`
		Action  string          `json:"action"`
		Query   json.RawMessage `json:"query"`
		Update  json.RawMessage `json:"update,omitempty"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Query) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_mutation_envelope")
		return
	}
	query, err := meldbase.DecodeQuerySpecJSON(envelope.Query, h.config.QueryLimits)
	if err != nil || query.HasModifiers() {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	collectionName, collection := r.PathValue("collection"), h.config.DB.Collection(r.PathValue("collection"))
	switch envelope.Action {
	case "updateOne", "updateMany":
		if len(envelope.Update) == 0 {
			writeError(w, http.StatusBadRequest, "missing_update")
			return
		}
		mutation, err := meldbase.DecodeMutationSpecJSON(envelope.Update, h.config.QueryLimits)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_update")
			return
		}
		policy, err := h.config.Authorizer.AuthorizeUpdate(r.Context(), actor, collectionName, query, mutation)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		policy = freezeUpdatePolicy(policy)
		query, err = applyUpdatePolicy(query, mutation, policy)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		var result meldbase.UpdateResult
		if envelope.Action == "updateOne" {
			result, err = collection.UpdateOneQuery(r.Context(), query, mutation)
		} else {
			result, err = collection.UpdateManyQueryLimited(r.Context(), query, mutation, policy.MaxAffected)
		}
		if err != nil {
			writeEngineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"version": 1, "matchedCount": result.MatchedCount, "modifiedCount": result.ModifiedCount})
	case "deleteOne", "deleteMany":
		if len(envelope.Update) != 0 {
			writeError(w, http.StatusBadRequest, "unexpected_update")
			return
		}
		policy, err := h.config.Authorizer.AuthorizeDelete(r.Context(), actor, collectionName, query)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		policy = freezeDeletePolicy(policy)
		query, err = applyDeletePolicy(query, policy)
		if err != nil {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		var result meldbase.DeleteResult
		if envelope.Action == "deleteOne" {
			result, err = collection.DeleteOneQuery(r.Context(), query)
		} else {
			result, err = collection.DeleteManyQueryLimited(r.Context(), query, policy.MaxAffected)
		}
		if err != nil {
			writeEngineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"version": 1, "deletedCount": result.DeletedCount})
	default:
		writeError(w, http.StatusBadRequest, "unknown_mutation_action")
	}
}

func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	var envelope struct {
		Version int             `json:"version"`
		Query   json.RawMessage `json:"query"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Query) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_query_envelope")
		return
	}
	query, err := meldbase.DecodeQuerySpecJSON(envelope.Query, h.config.QueryLimits)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	policy, err := h.authorizeQuery(r.Context(), actor, r.PathValue("collection"), query)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	query, err = applyPolicy(query, policy)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	var encoded []json.RawMessage
	authorized, err := underQueryPolicy(policy, func() error {
		cursor, err := h.config.DB.Collection(r.PathValue("collection")).FindQuery(r.Context(), query)
		if err != nil {
			return err
		}
		defer cursor.Close()
		budget, err := newWirePayloadBudget(h.config.MaxQueryResultBytes, httpQueryEnvelopeBytes)
		if err != nil {
			return err
		}
		encoded = make([]json.RawMessage, 0)
		for {
			document, exists, err := cursor.Next(r.Context())
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			raw, err := meldbase.MarshalWireDocument(project(document, policy))
			if err != nil {
				return err
			}
			if err := budget.add(raw); err != nil {
				return err
			}
			encoded = append(encoded, raw)
		}
	})
	if !authorized {
		writeError(w, http.StatusForbidden, "policy_expired")
		return
	}
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": 1, "documents": encoded})
}

// count returns a policy-capped lower bound. It deliberately reuses the
// regular query authorization path: a count must never reveal rows a caller
// could not query, nor bypass a publication's result budget.
func (h *Handler) count(w http.ResponseWriter, r *http.Request) {
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil || meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var envelope struct {
		Version int             `json:"version"`
		Query   json.RawMessage `json:"query"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Query) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_query_envelope")
		return
	}
	query, err := meldbase.DecodeQuerySpecJSON(envelope.Query, h.config.QueryLimits)
	if err != nil || query.HasModifiers() {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	policy, err := h.authorizeQuery(r.Context(), actor, r.PathValue("collection"), query)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	query, err = applyPolicy(query, policy)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	count := 0
	authorized, err := underQueryPolicy(policy, func() error {
		cursor, err := h.config.DB.Collection(r.PathValue("collection")).FindQuery(r.Context(), query)
		if err != nil {
			return err
		}
		defer cursor.Close()
		for {
			_, exists, err := cursor.Next(r.Context())
			if err != nil || !exists {
				return err
			}
			count++
		}
	})
	if !authorized {
		writeError(w, http.StatusForbidden, "policy_expired")
		return
	}
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": 1, "count": count, "capped": count == policy.MaxResults})
}

// groupCount is a deliberately narrow aggregate: one permitted top-level
// field and a count per distinct value. It shares query policy and result
// budgets with document reads, so dashboard summaries cannot become a data
// exfiltration side channel.
func (h *Handler) groupCount(w http.ResponseWriter, r *http.Request) {
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	body, err := readBounded(w, r, h.config.MaxBodyBytes)
	if err != nil || meldbase.ValidateStrictJSON(body, h.config.MaxBodyBytes) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var envelope struct {
		Version int             `json:"version"`
		Query   json.RawMessage `json:"query"`
		GroupBy string          `json:"groupBy"`
	}
	if err := decodeStrict(body, &envelope); err != nil || envelope.Version != 1 || len(envelope.Query) == 0 || envelope.GroupBy == "" || strings.Contains(envelope.GroupBy, ".") {
		writeError(w, http.StatusBadRequest, "invalid_aggregate")
		return
	}
	query, err := meldbase.DecodeQuerySpecJSON(envelope.Query, h.config.QueryLimits)
	if err != nil || query.HasModifiers() {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}
	policy, err := h.authorizeQuery(r.Context(), actor, r.PathValue("collection"), query)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	if !policy.AllowAllAggregateFields {
		if _, allowed := policy.AllowedAggregateFields[envelope.GroupBy]; !allowed {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
	}
	// Group keys are themselves returned data. A field that is only queryable
	// may be safe to test for equality but not safe to enumerate, so require
	// the ordinary result-field grant as well.
	if !policy.AllowAllResultFields {
		if _, allowed := policy.AllowedResultFields[envelope.GroupBy]; !allowed {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
	}
	query, err = applyPolicy(query, policy)
	if err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	type group struct {
		Key   json.RawMessage `json:"key"`
		Count int             `json:"count"`
	}
	groups := make(map[string]*group)
	examined := 0
	authorized, err := underQueryPolicy(policy, func() error {
		cursor, err := h.config.DB.Collection(r.PathValue("collection")).FindQuery(r.Context(), query)
		if err != nil {
			return err
		}
		defer cursor.Close()
		for {
			document, exists, err := cursor.Next(r.Context())
			if err != nil || !exists {
				return err
			}
			examined++
			value, exists := document[envelope.GroupBy]
			if !exists {
				value = meldbase.Null()
			}
			raw, err := meldbase.MarshalWireValue(value)
			if err != nil {
				return err
			}
			key := string(raw)
			item := groups[key]
			if item == nil {
				if len(groups) >= 100 {
					return meldbase.ErrResourceLimit
				}
				item = &group{Key: json.RawMessage(raw)}
				groups[key] = item
			}
			item.Count++
		}
	})
	if !authorized {
		writeError(w, http.StatusForbidden, "policy_expired")
		return
	}
	if err != nil {
		writeEngineError(w, err)
		return
	}
	result := make([]group, 0, len(groups))
	for _, item := range groups {
		result = append(result, *item)
	}
	// Map iteration is intentionally randomized by Go. A canonical key order
	// keeps dashboard output, cache keys, and SDK tests deterministic.
	sort.Slice(result, func(left, right int) bool {
		return string(result[left].Key) < string(result[right].Key)
	})
	budget, err := newWirePayloadBudget(h.config.MaxQueryResultBytes, httpQueryEnvelopeBytes)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	for _, item := range result {
		raw, err := json.Marshal(item)
		if err != nil {
			writeEngineError(w, err)
			return
		}
		if err := budget.add(raw); err != nil {
			writeEngineError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": 1, "groups": result, "capped": examined == policy.MaxResults})
}

func (h *Handler) authorizeQuery(ctx context.Context, actor Actor, collection string, query meldbase.QuerySpec) (QueryPolicy, error) {
	base, err := h.config.Authorizer.AuthorizeQuery(ctx, actor, collection, query)
	if err != nil {
		return QueryPolicy{}, err
	}
	base = freezeQueryPolicy(base)
	if base.Lease != nil && !base.Lease.Valid() {
		return QueryPolicy{}, ErrForbidden
	}
	if _, err := applyPolicy(query, base); err != nil {
		return QueryPolicy{}, err
	}
	if h.config.QueryPolicyResolver == nil {
		return base, nil
	}
	additional, found, err := h.config.QueryPolicyResolver.ResolveQueryPolicy(ctx, actor, collection, query)
	if err != nil {
		return QueryPolicy{}, ErrForbidden
	}
	if !found {
		return base, nil
	}
	return intersectQueryPolicies(base, freezeQueryPolicy(additional))
}

func (h *Handler) issueTicket(w http.ResponseWriter, r *http.Request) {
	actor, err := h.config.Authenticator.AuthenticateHTTP(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	ticket, err := h.tickets.issue(actor)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	w.Header().Set("cache-control", "no-store")
	response := map[string]any{"url": h.config.PublicRealtimeURL, "ticket": ticket}
	if requestsRealtimeCapabilities(r) {
		response["protocol"] = realtimeProtocolDescriptor(h.config)
	}
	writeJSON(w, http.StatusOK, response)
}
