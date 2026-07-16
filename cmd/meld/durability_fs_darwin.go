//go:build darwin

package main

import "syscall"

func durabilityFilesystemName(filesystem syscall.Statfs_t) string {
	bytes := make([]byte, 0, len(filesystem.Fstypename))
	for _, value := range filesystem.Fstypename {
		if value == 0 {
			break
		}
		bytes = append(bytes, byte(value))
	}
	return string(bytes)
}
