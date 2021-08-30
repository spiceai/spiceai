# Spice.ai Roadmap

This describes the current Spice.ai Roadmap.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Current Limitations

Spice.ai is still under early development, so there are several limitations, including:

- Data Sources are polled/fetched only (every 15 secs) - will change to push/streaming in v0.2
- Basic local Pod/Flight monitoring through polling - will move to streaming websockets in v0.2
- Data Connectors do not yet fully honor period, interval and granularity in all cases
- Single AI backend only (Tensorflow) - expect to support others like PyTorch and Scikit-learn in v0.2
- Running in Docker is required - a pure metal experience will be supported before v1.0
- Only macOS and Linux are supported natively. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- darwin/arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon 👨‍💻

### Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## v0.1-alpha Public Release Roadmap

- Basic package manager support (E.g. brew install spiceai)
- More samples

## v0.2-alpha Roadmap

- First-class simulated data
- Pluggable environments
- Additional AI algorithms (E.g. A3C)
- Multiple AI Engine backends (E.g. PyTorch, Scikit-learn, etc.)
- Search, index, publish and browse the Spice Rack registry
- Self-host on baremetal or VM
- Push/Streaming Data Sources/Connectors
- Local Pod/Flight monitoring (WebSockets)
- Custom visualization hooks for DataSources
- CI/CD on GitHub
- Sidecar injection on Kubernetes

## Spice.ai v1.0-stable Roadmap

- A/B testing and flighting
- Distributed learning

## Beyond v1.0

Coming soon!
