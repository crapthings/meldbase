package database

import (
	"encoding/json"
	"fmt"
	"strings"
)

func DecodeMutationSpecJSON(data []byte, limits QueryLimits) (MutationSpec, error) {
	limits = normalizedLimits(limits)
	if len(data) > limits.MaxWireBytes {
		return MutationSpec{}, fmt.Errorf("%w: mutation exceeds wire limit", ErrInvalidUpdate)
	}
	if err := validateJSON(data); err != nil {
		return MutationSpec{}, fmt.Errorf("%w: %v", ErrInvalidUpdate, err)
	}
	fields, err := rawObject(data)
	if err != nil {
		return MutationSpec{}, fmt.Errorf("%w: mutation must be object", ErrInvalidUpdate)
	}
	if err := allowOnly(fields, "version", "operations"); err != nil {
		return MutationSpec{}, err
	}
	var version int
	if err := updateRequired(fields, "version", &version); err != nil || version != 1 {
		return MutationSpec{}, fmt.Errorf("%w: unsupported mutation version", ErrInvalidUpdate)
	}
	var rawOperations []json.RawMessage
	if err := updateRequired(fields, "operations", &rawOperations); err != nil || len(rawOperations) == 0 || len(rawOperations) > limits.MaxNodes {
		return MutationSpec{}, fmt.Errorf("%w: operations outside bounds", ErrInvalidUpdate)
	}
	operations := make([]updateOperation, len(rawOperations))
	paths := []string{}
	for i, raw := range rawOperations {
		operationFields, err := rawObject(raw)
		if err != nil {
			return MutationSpec{}, fmt.Errorf("%w: operation must be object", ErrInvalidUpdate)
		}
		var kind, path string
		if err := updateRequired(operationFields, "op", &kind); err != nil {
			return MutationSpec{}, err
		}
		if err := updateRequired(operationFields, "path", &path); err != nil {
			return MutationSpec{}, err
		}
		if err := validateMutationPath(path, paths); err != nil {
			return MutationSpec{}, err
		}
		paths = append(paths, path)
		if kind == "unset" {
			if err := allowOnly(operationFields, "op", "path"); err != nil {
				return MutationSpec{}, err
			}
			operations[i] = updateOperation{kind: kind, path: path, value: Null()}
			continue
		}
		if kind != "set" && kind != "inc" && kind != "push" && kind != "pull" {
			return MutationSpec{}, fmt.Errorf("%w: unknown operation %q", ErrInvalidUpdate, kind)
		}
		if err := allowOnly(operationFields, "op", "path", "value"); err != nil {
			return MutationSpec{}, err
		}
		rawValue, exists := operationFields["value"]
		if !exists {
			return MutationSpec{}, fmt.Errorf("%w: missing value", ErrInvalidUpdate)
		}
		value, err := decodeWireValue(rawValue, limits, 0)
		if err != nil {
			return MutationSpec{}, fmt.Errorf("%w: invalid value", ErrInvalidUpdate)
		}
		if wireValueBytes(value) > limits.MaxValueBytes {
			return MutationSpec{}, fmt.Errorf("%w: value too large", ErrInvalidUpdate)
		}
		if kind == "inc" && value.kind != Int64Kind && value.kind != Float64Kind {
			return MutationSpec{}, fmt.Errorf("%w: inc requires numeric value", ErrInvalidUpdate)
		}
		operations[i] = updateOperation{kind: kind, path: path, value: value}
	}
	return MutationSpec{operations: operations}, nil
}

func validateMutationPath(path string, previous []string) error {
	if err := validatePath(path); err != nil {
		return fmt.Errorf("%w: invalid mutation path", ErrInvalidUpdate)
	}
	if path == "_id" || strings.HasPrefix(path, "_id.") {
		return ErrImmutableID
	}
	for _, existing := range previous {
		if existing == path || strings.HasPrefix(existing, path+".") || strings.HasPrefix(path, existing+".") {
			return fmt.Errorf("%w: conflicting paths %q and %q", ErrInvalidUpdate, existing, path)
		}
	}
	return nil
}

func allowOnly(fields map[string]json.RawMessage, allowed ...string) error {
	set := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		set[field] = struct{}{}
	}
	for field := range fields {
		if _, ok := set[field]; !ok {
			return fmt.Errorf("%w: unexpected field %q", ErrInvalidUpdate, field)
		}
	}
	return nil
}
func updateRequired[T any](fields map[string]json.RawMessage, name string, target *T) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("%w: missing %s", ErrInvalidUpdate, name)
	}
	if err := strictJSON(raw, target); err != nil {
		return fmt.Errorf("%w: invalid %s", ErrInvalidUpdate, name)
	}
	return nil
}
