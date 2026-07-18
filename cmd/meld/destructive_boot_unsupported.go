//go:build !linux

package main

import "errors"

func destructiveBootID() (string, error) {
	return "", errors.New("power-cut qualification is supported only on Linux")
}
