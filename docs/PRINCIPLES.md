# Spice.ai First Principles

Spice.ai is built upon a foundation of first principles.

## Summary

- Developer experience first
- Bring data and ML to your application
- API first
- Composable from community driven-components
- First class extensibility
- Leverage model versioning

### Developer experience first

The goal of Spice.ai is to make creating intelligent applications as easy as possible, and so developer experience comes first

### Bring data and ML to your application

Instead of sending valuable application data into another service, bring data and ML as close to your application as possible. This supports the approach for the application and the ML engine to work together in the same data space, and to provide feedback to each other, for higher performance.

### API first

All functionality is available through the HTTP API on the Spice.ai runtime: `spiced`

### Composable from community driven-components

A Spice.ai project consists of Datasets and Models, which are composable through community built-components available through the spicerack.org registry or defined locally.

### First class extensibility

Spice.ai is designed to be extended. All components are behind well-defined interfaces, and new components can be added dynamically without code changes to the core project.

### Leverage model versioning

Spice.ai supports pulling the models in specific versions defined in a Spicepod, which ensures your ML applications are comparable and traceable.
