//go:build !linux

package main

import "errors"

// destructiveBootID reports that power-cut qualification requires Linux.
func destructiveBootID() (string, error) {
	return "", errors.New("power-cut qualification is supported only on Linux")
}
