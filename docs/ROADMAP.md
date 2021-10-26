# Spice.ai Roadmap

This describes the current Spice.ai roadmap.

This is a living doc that will be updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Current Limitations

Spice.ai is still under early development, so there are several gaps and limitations, including:

- Simulated environment support is very limited
- Splitting of data (training/testing/etc)
- Custom environments are not yet supported
- Custom visualizations are not yet supported
- Environment and reward function code must be written in Python 3. The plan is to enable these to be written in any language that compiles to Web Assembly.
- Basic local Pod/Flight monitoring through polling - will move to streaming websockets
- Single ML backend (Tensorflow) - expect to support others like PyTorch and Scikit-learn
- Running in Docker is required - a pure metal experience will be supported before v1.0 (possible now, but unsupported)
- Native support for macOS and Linux only. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- darwin/arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon 👨‍💻👩‍💻

### Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## Tentative v0.4-alpha (Nov, 2021) roadmap

- Improved data visualization in dashboard
- Additional ML algorithm (A2C/A3C or SAC)
- Externally editable and testable reward functions (Python)

## Tentative v0.5-alpha (Dec, 2021) roadmap

- Self-host on baremetal or VM
- Local Pod/Flight monitoring (WebSockets)
- Multiple AI Engine backends (E.g. PyTorch, Scikit-learn, etc.)
- First-class simulated environment
- Pluggable environments

## Tentative v0.6-alpha (Jan, 2022) roadmap

- Search, index, publish and browse the Spice Rack registry
- CI/CD on GitHub
- Custom visualization hooks for Dataspaces
- Sidecar injection on Kubernetes

## v1.0-stable roadmap

- A/B testing and flighting
- Distributed learning

## Beyond v1.0

Based on community feedback!
