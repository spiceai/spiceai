# Spice.ai Roadmap

This describes the current Spice.ai roadmap.

This is a living doc that will be updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Current Limitations

Spice.ai is still under early development, so there are several gaps and limitations, including:

- Reward code must be written in Python 3. The plan is to enable these to be written in any language that compiles to Web Assembly.
- Running in Docker is required - a pure metal experience will be supported before v1.0 (possible now, but unsupported).
- Native support for macOS and Linux only. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- darwin/arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon 👨‍💻👩‍💻

### Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## Tentative v0.7-alpha roadmap

- Darwin/ARM64 support
- AI engine migration from pandas to pyarrow
- Goals - E.g. maximize "a" or minimize "b"

## Features being considered for v1.0-stable

- WebSocket support for dashboard data
- CI/CD on GitHub
- Self-host on baremetal or VM
- Pluggable environments
- Search, index, publish and browse the Spice Rack registry
- Distributed learning
- Improved data visualization in dashboard
- Custom visualization hooks for Dataspaces
- Parallel Actions (AI Engine)
- A/B testing and flighting
- Continuous Actions

## Beyond v1.0

Based on community feedback!
