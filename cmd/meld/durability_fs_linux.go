//go:build linux

package main

import (
	"fmt"
	"syscall"
)

func durabilityFilesystemName(filesystem syscall.Statfs_t) string {
	switch uint64(filesystem.Type) {
	case 0xef53:
		return "ext-family"
	case 0x58465342:
		return "xfs"
	case 0x9123683e:
		return "btrfs"
	case 0x01021994:
		return "tmpfs"
	case 0x6969:
		return "nfs"
	case 0x794c7630:
		return "overlayfs"
	case 0x2fc12fc1:
		return "zfs"
	case 0x65735546:
		return "fuse"
	default:
		return fmt.Sprintf("unknown-%x", uint64(filesystem.Type))
	}
}
