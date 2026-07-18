//go:build !linux

package main

import "errors"

func destructivePathIsMountRoot(string) (bool, error) {
	return false, errors.New("destructive volume qualification is supported only on Linux")
}
