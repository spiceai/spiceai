# Spice.ai First Principles

Spice is built upon a foundation of first principles.

## Summary

- Secure by default
- Developer experience first
- Bring data and ML to your application
- API first
- Composable from community driven-components
- First class extensibility
- Leverage model versioning
- Align to industry standards

### Secure by default

Spice should be secure by default, with optional opt-in settings that can make the runtime run insecurely if needed.

Example:
Spice defaults to connecting to remote data sources over encrypted channels, like TLS/SSL, with an option to connect insecurely if the remote server does not support TLS connections.

### Developer experience first

The goal of Spice is to make creating intelligent applications as easy as possible, and so developer experience comes first

### Bring data and ML to your application

Instead of sending valuable application data into another service, bring data and ML as close to your application as possible. This supports the approach for the application and the ML engine to work together in the same data space, and to provide feedback to each other, for higher performance.

### API first

All functionality is available through the HTTP API on the Spice runtime: `spiced`

### Composable from community driven-components

A Spice project consists of Datasets and Models, which are composable through community built-components available through the spicerack.org registry or defined locally.

### First class extensibility

Spice is designed to be extended. All components are behind well-defined interfaces, and new components can be added dynamically without code changes to the core project.

### Leverage model versioning

Spice supports pulling the models in specific versions defined in a Spicepod, which ensures your ML applications are comparable and traceable.

### Align to industry standards

Spice uses industry standards like Apache Arrow and ONNX to improve performance, compatibility, and usability by leveraging open-source innovations.
