package database

import "fmt"

// containsQueryValue reports whether a list includes an equal value.
func containsQueryValue(values []Value, candidate Value) bool {
	for _, value := range values {
		if value.Equal(candidate) {
			return true
		}
	}
	return false
}

// queryArraySize accepts integer array lengths within the supported range.
func queryArraySize(raw any) (int64, bool) {
	var size int64
	switch value := raw.(type) {
	case int:
		size = int64(value)
	case int8:
		size = int64(value)
	case int16:
		size = int64(value)
	case int32:
		size = int64(value)
	case int64:
		size = value
	case uint:
		if uint64(value) > uint64(maxQueryArraySize) {
			return 0, false
		}
		size = int64(value)
	case uint8:
		size = int64(value)
	case uint16:
		size = int64(value)
	case uint32:
		size = int64(value)
	case uint64:
		if value > uint64(maxQueryArraySize) {
			return 0, false
		}
		size = int64(value)
	default:
		return 0, false
	}
	return size, size >= 0 && size <= maxQueryArraySize
}

// queryTypeOperand reads a type name or a bounded list of type names.
func queryTypeOperand(raw any, maxItems int) ([]string, error) {
	switch value := raw.(type) {
	case string:
		return []string{value}, nil
	case []string:
		if len(value) == 0 || len(value) > maxItems {
			return nil, fmt.Errorf("%w: $type expects a non-empty bounded type list", ErrInvalidFilter)
		}
		return append([]string(nil), value...), nil
	case []any:
		if len(value) == 0 || len(value) > maxItems {
			return nil, fmt.Errorf("%w: $type expects a non-empty bounded type list", ErrInvalidFilter)
		}
		result := make([]string, len(value))
		for index, item := range value {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%w: $type names must be strings", ErrInvalidFilter)
			}
			result[index] = name
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%w: $type expects a type name or type-name array", ErrInvalidFilter)
	}
}

// queryValueOf converts a filter value while enforcing the special document ID rules.
func queryValueOf(path string, raw any) (Value, error) {
	if path == "_id" {
		switch value := raw.(type) {
		case DocumentID:
			return ID(value), nil
		case string:
			id, err := ParseDocumentID(value)
			if err != nil {
				return Value{}, err
			}
			if id.IsZero() || id.String() != value {
				return Value{}, fmt.Errorf("_id must be a non-zero canonical string")
			}
			return ID(id), nil
		case Value:
			if value.kind == IDKind {
				return value.Clone(), nil
			}
		}
		return Value{}, fmt.Errorf("_id requires DocumentID or canonical string")
	}
	value, err := ValueOf(raw)
	if err != nil {
		return Value{}, err
	}
	if err := validateArray([]Value{value}, 0); err != nil {
		return Value{}, err
	}
	return value, nil
}

// asFilter reads a filter from either supported map type.
func asFilter(raw any) (Filter, bool) {
	if value, ok := raw.(Filter); ok {
		return value, true
	}
	if value, ok := raw.(map[string]any); ok {
		return Filter(value), true
	}
	return nil, false
}

// asFilters reads a list of filters and rejects non-filter items.
func asFilters(raw any) ([]Filter, error) {
	if values, ok := raw.([]Filter); ok {
		return values, nil
	}
	items, ok := asAnySlice(raw)
	if !ok {
		return nil, fmt.Errorf("not an array")
	}
	result := make([]Filter, len(items))
	for i, item := range items {
		value, ok := asFilter(item)
		if !ok {
			return nil, fmt.Errorf("item is not filter")
		}
		result[i] = value
	}
	return result, nil
}

// asAnySlice reads an array of raw values or database values.
func asAnySlice(raw any) ([]any, bool) {
	if values, ok := raw.([]any); ok {
		return values, true
	}
	if values, ok := raw.([]Value); ok {
		result := make([]any, len(values))
		for i := range values {
			result[i] = values[i]
		}
		return result, true
	}
	return nil, false
}

// asStringAnyMap reads an object from either supported map type.
func asStringAnyMap(raw any) (map[string]any, bool) {
	if value, ok := raw.(map[string]any); ok {
		return value, true
	}
	if value, ok := raw.(Filter); ok {
		return map[string]any(value), true
	}
	return nil, false
}
