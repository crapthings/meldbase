package database

import (
	"fmt"
	"sort"
)

// compileElementFilter turns scalar array-element conditions into a query expression.
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
