# Spice.ai First Principles

Spice.ai is built upon a foundation of first principles.

## Summary

- Data is always in time-based intervals

### Data is always in time-based intervals

- All data is based on [Unix Time](https://en.wikipedia.org/wiki/Unix_time) at `second` granularity
- All data is assumed to have `time`, `start` and `end`. `time` is the aggegration point, and if either `start` or `end` is missing, they are considered equal with `time`
