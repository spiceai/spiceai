# Spice AI Roadmap

This describes the current Spice AI Roadmap.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Current Limitations

- Data Sources are polled only (every 30 secs) - will change to push/streaming

### Known bugs

- Data Connectors do not honor interval

### v0.1-alpha Developer Preview limitations

- Docker hosting only
- Windows baremetal host requires WSL 2
- Single AI backend (Tensorflow)
- Development Registry (github.com/spiceai/registry)
- Basic local Pod/Flight monitoring through polling

## v0.1-alpha Public Release Roadmap

- Self-host on baremetal or VM
- Kubernetes-host
- Push/Streaming Data Sources/Connectors
- CI/CD on GitHub
- Additional AI algorithms (E.g. A3C)
- Defined DataSource/DataProcessor interface with community implementation repo
- Spice AI Registry v0.1
- Local Pod/Flight monitoring (WebSockets)
- Custom visualization hooks for DataSources

## v0.2.-alpha Roadmap

- Multiple AI Engine backends (E.g. PyTorch, Scikit-learn, etc.)

## Spice AI v1.0-stable Roadmap

- A/B testing
- Distributed learning

## Beyond v1.0

Coming soon!
