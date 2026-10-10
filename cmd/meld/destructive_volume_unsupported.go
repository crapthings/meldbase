//go:build !linux

package main

import "errors"

// destructivePathIsMountRoot reports that destructive volume qualification requires Linux.
func destructivePathIsMountRoot(string) (bool, error) {
	return false, errors.New("destructive volume qualification is supported only on Linux")
}
