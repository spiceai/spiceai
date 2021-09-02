# Spice.ai Roadmap

This describes the current Spice.ai roadmap.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Current Limitations

Spice.ai is still under early development, so there are several gaps and limitations, including:

- Simulated environment support is very limited
- Custom environments are not yet supported
- Custom visualizations are not yet support
- Environment and reward function code must be written in Python 3
- Data Sources are polled/fetched only - will change to push/streaming in v0.2
- Basic local Pod/Flight monitoring through polling - will move to streaming websockets in v0.2
- Data Connectors do not yet fully honor period, interval and granularity in all cases
- Single AI backend only (Tensorflow) - expect to support others like PyTorch and Scikit-learn in v0.3
- Running in Docker is required - a pure metal experience will be supported before v1.0
- Only macOS and Linux are supported natively. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- darwin/arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon 👨‍💻

### Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## Tentative v0.2-alpha roadmap

- First-class simulated environment
- Pluggable environments
- Additional AI algorithms (E.g. A3C)
- Search, index, publish and browse the Spice Rack registry
- Push/Streaming Data Sources/Connectors
- Local Pod/Flight monitoring (WebSockets)
- Custom visualization hooks for DataSources

## Tentative v0.3-alpha roadmap

- Multiple AI Engine backends (E.g. PyTorch, Scikit-learn, etc.)
- Self-host on baremetal or VM
- CI/CD on GitHub
- Sidecar injection on Kubernetes

## v1.0-stable roadmap

- A/B testing and flighting
- Distributed learning

## Beyond v1.0

Coming soon!
