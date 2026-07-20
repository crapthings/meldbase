//go:build linux

package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func destructivePathIsMountRoot(path string) (bool, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("read mount namespace: %w", err)
	}
	defer file.Close()
	clean := filepath.Clean(path)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 {
			return false, errors.New("invalid /proc/self/mountinfo record")
		}
		separator := -1
		for index, field := range fields {
			if field == "-" {
				separator = index
				break
			}
		}
		if separator < 6 || separator+3 >= len(fields) {
			return false, errors.New("invalid /proc/self/mountinfo separator")
		}
		mountPoint, err := decodeMountInfoPath(fields[4])
		if err != nil {
			return false, err
		}
		if filepath.Clean(mountPoint) == clean {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func decodeMountInfoPath(value string) (string, error) {
	var result strings.Builder
	result.Grow(len(value))
	for index := 0; index < len(value); {
		if value[index] != '\\' {
			result.WriteByte(value[index])
			index++
			continue
		}
		if index+3 >= len(value) {
			return "", errors.New("invalid mountinfo path escape")
		}
		decoded, err := strconv.ParseUint(value[index+1:index+4], 8, 8)
		if err != nil {
			return "", errors.New("invalid mountinfo octal escape")
		}
		result.WriteByte(byte(decoded))
		index += 4
	}
	return result.String(), nil
}
