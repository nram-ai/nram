# Download and Install

Prebuilt packages are published for every platform as ready-to-run archives, so you can run nram without installing Go or Node. Each contains a single self-contained binary with the Web Console embedded.

Back to the [README](../README.md). The steps below get the binary onto your machine and start it. Once it is running, open `http://localhost:8674` and pick up the [Quick Start at the setup wizard](quickstart.md#4-open-the-setup-wizard) to create your admin account and configure a provider, the build and run steps before it are only for building from source.

> These are **nightly** builds: they track `master`, are refreshed every night, and are pre-releases (bleeding edge, not a stable version). Tagged stable releases, once published, appear on the [Releases page](https://github.com/nram-ai/nram/releases).

Every nightly asset lives under one rolling pre-release with stable URLs at [`releases/tag/nightly`](https://github.com/nram-ai/nram/releases/tag/nightly).

## macOS

nram on macOS is a terminal server, not a GUI app: download the `.tar.gz`, extract the binary, and run it from a terminal. Builds are published for `arm64` (Apple Silicon) and `amd64` (Intel). They are not code-signed or notarized, so a download is quarantined by Gatekeeper. Clear the quarantine flag once (this is what resolves the **"damaged / cannot be opened"** message) and run it:

```bash
# Apple Silicon; swap arm64 -> amd64 for Intel Macs
curl -fL -o nram-macos.tar.gz https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_darwin_arm64.tar.gz
tar -xzf nram-macos.tar.gz
xattr -d com.apple.quarantine nram   # clears the Gatekeeper "damaged / cannot be opened" block
chmod +x nram
./nram
```

## Linux

A native package (`.deb` or `.rpm`, which installs a desktop launcher and icons) or a `.tar.gz` archive, for `amd64` or `arm64`.

```bash
# Debian / Ubuntu (amd64)
curl -fLO https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_linux_amd64.deb
sudo apt install ./nram_nightly_linux_amd64.deb && nram
```

```bash
# Fedora / RHEL (amd64)
curl -fLO https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_linux_amd64.rpm
sudo dnf install ./nram_nightly_linux_amd64.rpm && nram
```

```bash
# Tarball, any distro (amd64)
curl -fL -o nram-linux.tar.gz https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_linux_amd64.tar.gz
tar -xzf nram-linux.tar.gz && ./nram
```

## Windows

Download [`nram_nightly_windows_amd64.zip`](https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_windows_amd64.zip) (or the `arm64` build), unzip it, and run `nram.exe`; the icon and version metadata are embedded. SmartScreen may warn on an unsigned binary, choose **More info → Run anyway**.

## Verify a download

Each release ships a `SHA256SUMS` manifest covering every asset. Download it alongside the file you grabbed and check it:

```bash
curl -fLO https://github.com/nram-ai/nram/releases/download/nightly/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS   # macOS: shasum -a 256 -c --ignore-missing SHA256SUMS
```

## Build from source instead

If you would rather build it yourself, see the [Quick Start](quickstart.md), which covers the prerequisites (Go, Node, npm) and the `make build` flow.
