package database

import (
	"fmt"
	"sort"
	"strings"
)

// Filter maps document fields and query operators to their conditions.
type Filter map[string]any

// QueryOptions configures query sorting and pagination.
type QueryOptions struct {
	Sort  []SortField
	Skip  int
	Limit *int
}

// CompileQuery validates a filter and options to produce a reusable query.
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

// filterCompiler tracks the limits and node count while compiling a filter.
type filterCompiler struct {
	limits QueryLimits
	nodes  int
}

// node counts a query node and rejects filters that exceed the limit.
func (c *filterCompiler) node() error {
	c.nodes++
	if c.nodes > c.limits.MaxNodes {
		return fmt.Errorf("%w: too many query nodes", ErrInvalidFilter)
	}
	return nil
}

// compileFilter turns document conditions into a query expression within the configured limits.
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

// compileField turns the conditions for one field into query expressions.
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
