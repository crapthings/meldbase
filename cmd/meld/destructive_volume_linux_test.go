//go:build linux

package main

import "testing"

func TestDecodeMountInfoPath(t *testing.T) {
	decoded, err := decodeMountInfoPath(`/mnt/meld\040base\134volume`)
	if err != nil || decoded != "/mnt/meld base\\volume" {
		t.Fatalf("decoded=%q err=%v", decoded, err)
	}
	for _, invalid := range []string{`/mnt/bad\`, `/mnt/bad\04`, `/mnt/bad\xyz`} {
		if _, err := decodeMountInfoPath(invalid); err == nil {
			t.Fatalf("invalid escape %q accepted", invalid)
		}
	}
}
