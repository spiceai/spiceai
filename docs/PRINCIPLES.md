# Spice.ai First Principles

Spice.ai is built upon a foundation of first principles.

## Summary

- Developer experience first
- API first
- Composable from community driven-components
- Data is always in time-based intervals

### Developer experience first

- The goal of Spice.ai is to make creating intelligent applications as easy as possible, and so developer experience comes first

### API first

- All functionality is available through the HTTP API on the Spice.ai runtime: `spiced`

### Composable from community driven-components

- A Spice.ai project consists of Pods, Data Connectors, and Data Processors, all of which are composable through community built-components available through the spicerack.org registry or the `data-components-contrib` reposistory.

### Data is always in time-based intervals

- All data is based on [Unix Time](https://en.wikipedia.org/wiki/Unix_time) at `second` granularity
- All data is assumed to have `time`, `start` and `end`. `time` is the aggegration point, and if either `start` or `end` is missing, they are considered equal with `time`
