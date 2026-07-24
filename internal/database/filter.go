package database

import (
	"fmt"
	"sort"
	"strings"
)

type Filter map[string]any

type QueryOptions struct {
	Sort  []SortField
	Skip  int
	Limit *int
}

func CompileQuery(filter Filter, options QueryOptions) (QuerySpec, error) {
	compiler := filterCompiler{limits: DefaultQueryLimits}
	where, err := compiler.compileFilter(filter, 0)
	if err != nil {
		return QuerySpec{}, err
	}
	if err := validateSortFields(options.Sort, compiler.limits); err != nil {
		return QuerySpec{}, err
	}
	if options.Skip < 0 {
		return QuerySpec{}, fmt.Errorf("%w: invalid skip", ErrInvalidFilter)
	}
	var limit *int
	if options.Limit != nil {
		if *options.Limit < 0 || *options.Limit > compiler.limits.MaxLimit {
			return QuerySpec{}, fmt.Errorf("%w: invalid limit", ErrInvalidFilter)
		}
		value := *options.Limit
		limit = &value
	}
	query := QuerySpec{where: where, sort: append([]SortField(nil), options.Sort...), skip: options.Skip, limit: limit}
	if err := validateQuerySpec(query, compiler.limits); err != nil {
		return QuerySpec{}, err
	}
	return query, nil
}

type filterCompiler struct {
	limits QueryLimits
	nodes  int
}

func (c *filterCompiler) node() error {
	c.nodes++
	if c.nodes > c.limits.MaxNodes {
		return fmt.Errorf("%w: too many query nodes", ErrInvalidFilter)
	}
	return nil
}

func (c *filterCompiler) compileFilter(filter Filter, depth int) (expr, error) {
	if filter == nil {
		filter = Filter{}
	}
	if depth > c.limits.MaxDepth {
		return nil, fmt.Errorf("%w: query nesting is too deep", ErrInvalidFilter)
	}
	keys := make([]string, 0, len(filter))
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]expr, 0, len(keys))
	for _, key := range keys {
		raw := filter[key]
		switch key {
		case "$and", "$or":
			parts, err := asFilters(raw)
			if err != nil || len(parts) == 0 || len(parts) > c.limits.MaxArrayItems {
				return nil, fmt.Errorf("%w: %s expects a non-empty bounded filter array", ErrInvalidFilter, key)
			}
			if err := c.node(); err != nil {
				return nil, err
			}
			children := make([]expr, len(parts))
			for i := range parts {
				children[i], err = c.compileFilter(parts[i], depth+1)
				if err != nil {
					return nil, err
				}
			}
			args = append(args, logicalExpr{op: key[1:], args: children})
		case "$not":
			part, ok := asFilter(raw)
			if !ok {
				return nil, fmt.Errorf("%w: $not expects a filter", ErrInvalidFilter)
			}
			if err := c.node(); err != nil {
				return nil, err
			}
			child, err := c.compileFilter(part, depth+1)
			if err != nil {
				return nil, err
			}
			args = append(args, notExpr{arg: child})
		default:
			if len(key) > 0 && key[0] == '$' {
				return nil, fmt.Errorf("%w: unknown logical operator %q", ErrInvalidFilter, key)
			}
			if err := validatePath(key); err != nil {
				return nil, err
			}
			compiled, err := c.compileField(key, raw, depth+1)
			if err != nil {
				return nil, err
			}
			args = append(args, compiled...)
		}
	}
	if len(args) == 0 {
		if err := c.node(); err != nil {
			return nil, err
		}
		return trueExpr{}, nil
	}
	if len(args) == 1 {
		return args[0], nil
	}
	if err := c.node(); err != nil {
		return nil, err
	}
	return logicalExpr{op: "and", args: args}, nil
}

func (c *filterCompiler) compileField(path string, raw any, depth int) ([]expr, error) {
	if depth > c.limits.MaxDepth {
		return nil, fmt.Errorf("%w: query nesting is too deep", ErrInvalidFilter)
	}
	operators, isMap := asStringAnyMap(raw)
	isOperators := false
	if isMap {
		for key := range operators {
			if len(key) > 0 && key[0] == '$' {
				isOperators = true
				break
			}
		}
	}
	if !isOperators {
		value, err := queryValueOf(path, raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
		if wireValueBytes(value) > c.limits.MaxValueBytes {
			return nil, fmt.Errorf("%w: query value is too large", ErrInvalidFilter)
		}
		if err := c.node(); err != nil {
			return nil, err
		}
		return []expr{compareExpr{cmp: "eq", path: path, value: value}}, nil
	}
	keys := make([]string, 0, len(operators))
	for key := range operators {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: empty operator object", ErrInvalidFilter)
	}
	result := make([]expr, 0, len(keys))
	for _, operator := range keys {
		operand := operators[operator]
		if err := c.node(); err != nil {
			return nil, err
		}
		switch operator {
		case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
			value, err := queryValueOf(path, operand)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
			}
			result = append(result, compareExpr{cmp: operator[1:], path: path, value: value})
		case "$exists":
			value, ok := operand.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: $exists expects bool", ErrInvalidFilter)
			}
			result = append(result, existsExpr{path: path, value: value})
		case "$size":
			size, ok := queryArraySize(operand)
			if !ok {
				return nil, fmt.Errorf("%w: $size expects a non-negative safe integer", ErrInvalidFilter)
			}
			result = append(result, sizeExpr{path: path, size: size})
		case "$type":
			names, err := queryTypeOperand(operand, c.limits.MaxArrayItems)
			if err != nil {
				return nil, err
			}
			types, err := normalizeQueryTypes(names, c.limits.MaxArrayItems)
			if err != nil {
				return nil, err
			}
			result = append(result, typeExpr{path: path, types: types})
		case "$all":
			items, ok := asAnySlice(operand)
			if !ok || len(items) == 0 || len(items) > c.limits.MaxArrayItems {
				return nil, fmt.Errorf("%w: $all expects a non-empty bounded array", ErrInvalidFilter)
			}
			values := make([]Value, 0, len(items))
			for _, item := range items {
				value, err := queryValueOf(path, item)
				if err != nil {
					return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
				}
				if !containsQueryValue(values, value) {
					values = append(values, value)
				}
			}
			result = append(result, allExpr{path: path, values: values})
		case "$elemMatch":
			element, ok := asStringAnyMap(operand)
			if !ok || len(element) == 0 {
				return nil, fmt.Errorf("%w: $elemMatch expects a non-empty object", ErrInvalidFilter)
			}
			scalar := false
			for key := range element {
				if strings.HasPrefix(key, "$") {
					scalar = true
					break
				}
			}
			if scalar {
				for key := range element {
					if !strings.HasPrefix(key, "$") {
						return nil, fmt.Errorf("%w: scalar $elemMatch cannot mix field keys and operators", ErrInvalidFilter)
					}
				}
				condition, err := c.compileElementFilter(path, element, depth+1)
				if err != nil {
					return nil, err
				}
				result = append(result, elemMatchExpr{path: path, mode: "scalar", scalar: condition})
				continue
			}
			condition, err := c.compileFilter(Filter(element), depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, elemMatchExpr{path: path, mode: "object", object: condition})
		case "$in", "$nin":
			items, ok := asAnySlice(operand)
			if !ok || len(items) > c.limits.MaxArrayItems {
				return nil, fmt.Errorf("%w: %s expects a bounded array", ErrInvalidFilter, operator)
			}
			values := make([]Value, len(items))
			for i := range items {
				value, err := queryValueOf(path, items[i])
				if err != nil {
					return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
				}
				values[i] = value
			}
			result = append(result, membershipExpr{op: operator[1:], path: path, values: values})
		case "$not":
			nested, err := c.compileField(path, operand, depth+1)
			if err != nil {
				return nil, err
			}
			var child expr
			if len(nested) == 1 {
				child = nested[0]
			} else {
				child = logicalExpr{op: "and", args: nested}
			}
			result = append(result, notExpr{arg: child})
		default:
			return nil, fmt.Errorf("%w: unknown field operator %q", ErrInvalidFilter, operator)
		}
	}
	return result, nil
}

func (c *filterCompiler) compileElementFilter(path string, filter map[string]any, depth int) (elementExpr, error) {
	if depth > c.limits.MaxDepth || len(filter) == 0 {
		return nil, fmt.Errorf("%w: invalid scalar $elemMatch", ErrInvalidFilter)
	}
	keys := make([]string, 0, len(filter))
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]elementExpr, 0, len(keys))
	for _, operator := range keys {
		operand := filter[operator]
		if err := c.node(); err != nil {
			return nil, err
		}
		switch operator {
		case "$and", "$or":
			items, ok := asAnySlice(operand)
			if !ok || len(items) == 0 || len(items) > c.limits.MaxArrayItems {
				return nil, fmt.Errorf("%w: %s expects a non-empty bounded condition array", ErrInvalidFilter, operator)
			}
			children := make([]elementExpr, len(items))
			for index, item := range items {
				child, ok := asStringAnyMap(item)
				if !ok {
					return nil, fmt.Errorf("%w: scalar $elemMatch conditions must be objects", ErrInvalidFilter)
				}
				var err error
				children[index], err = c.compileElementFilter(path, child, depth+1)
				if err != nil {
					return nil, err
				}
			}
			args = append(args, elementLogicalExpr{op: operator[1:], args: children})
		case "$not":
			child, ok := asStringAnyMap(operand)
			if !ok {
				return nil, fmt.Errorf("%w: scalar $elemMatch $not expects an object", ErrInvalidFilter)
			}
			condition, err := c.compileElementFilter(path, child, depth+1)
			if err != nil {
				return nil, err
			}
			args = append(args, elementNotExpr{arg: condition})
		case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
			value, err := queryValueOf(path, operand)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
			}
			args = append(args, elementCompareExpr{cmp: operator[1:], value: value})
		case "$in", "$nin":
			items, ok := asAnySlice(operand)
			if !ok || len(items) > c.limits.MaxArrayItems {
				return nil, fmt.Errorf("%w: %s expects a bounded array", ErrInvalidFilter, operator)
			}
			values := make([]Value, len(items))
			for index, item := range items {
				value, err := queryValueOf(path, item)
				if err != nil {
					return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
				}
				values[index] = value
			}
			args = append(args, elementMembershipExpr{op: operator[1:], values: values})
		default:
			return nil, fmt.Errorf("%w: unsupported scalar $elemMatch operator %q", ErrInvalidFilter, operator)
		}
	}
	if len(args) == 1 {
		return args[0], nil
	}
	return elementLogicalExpr{op: "and", args: args}, nil
}

func containsQueryValue(values []Value, candidate Value) bool {
	for _, value := range values {
		if value.Equal(candidate) {
			return true
		}
	}
	return false
}

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

func asFilter(raw any) (Filter, bool) {
	if value, ok := raw.(Filter); ok {
		return value, true
	}
	if value, ok := raw.(map[string]any); ok {
		return Filter(value), true
	}
	return nil, false
}
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
func asStringAnyMap(raw any) (map[string]any, bool) {
	if value, ok := raw.(map[string]any); ok {
		return value, true
	}
	if value, ok := raw.(Filter); ok {
		return map[string]any(value), true
	}
	return nil, false
}
