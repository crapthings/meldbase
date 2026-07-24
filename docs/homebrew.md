# Homebrew alpha tap

Meldbase does not belong in `homebrew/core` while it is an alpha project. The
maintainer-owned tap distributes versioned, checksummed macOS CLI archives for
the current alpha line instead.

After the first alpha release and Formula update, install it with:

```sh
brew tap crapthings/tap
brew trust --tap crapthings/tap
brew install crapthings/tap/meldbase
meld version
```

The tap Formula selects either the Apple Silicon or Intel archive, verifies its
SHA-256 digest, and installs only the `meld` CLI. It does not install a server,
create a data directory, configure authentication, or make the single-node
database highly available. Follow [single-node deployment](/production-single-node)
before operating durable data.

Every `v0.x.y-alpha.N` tag must point at the current `main` commit. The release
workflow runs Go verification, produces both macOS archives, creates a GitHub
Release with `checksums.txt` and the rendered `meldbase.rb`, and attaches a
keyless provenance attestation. The maintainer then updates
`crapthings/homebrew-tap` with the rendered Formula. Verify an installed alpha
with `meld version`; use the tag and archive checksum from the GitHub Release
when recording deployment evidence.

Homebrew 6 requires explicit trust for Formulae from third-party taps. Trust
only the `crapthings/tap` repository after reviewing its Formula and release
assets; the command above grants trust to that tap, not to arbitrary Formulae.
