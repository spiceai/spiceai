# Principles

## Overview

- Beta internally QA complete; RC externally QA complete.
- Documentation quality should match code quality standard.

## Scope

A "component" refers to any of the following:

- Data Connectors — see [connectors/](./connectors/).
- Data Accelerators — see [accelerators/](./accelerators/).
- Catalog Connectors — see [catalogs/](./catalogs/).
- Models (LLM providers) — see [models/](./models/).
- Embedding providers — see [embeddings/](./embeddings/).

Runtime subsystems and cross-cutting capabilities that are not a single component (e.g. search, HA, distributed query) use the generic [features/](./features/) criteria.

The generic feature criteria and the component-specific criteria are complementary:

- **Component criteria apply first.** Connector/accelerator/catalog/model/embedding releases are governed by the files in their respective subdirectories.
- **Feature criteria apply to component releases as well**, wherever the component-specific criteria do not already cover the same requirement (e.g. preview-warning removal, deprecation policy). Where the two conflict, the component criteria win.

## Per Level

### Alpha

- Code complete at quality benchmark for most basic, valuable functionality.
- Limitations & edge case failures expected.

### Beta

- Functionally complete.
- Satisfies minimal performance bar. Resource efficiency (memory, CPU, and I/O usage) is measured.
- Metrics & tracing in place for observability.

### RC

- Ready for production: security & privacy.
- Performance tested at scale.
- Feature complete; API stable.
- Components ensure correctness.

### Stable

- Verified in production.
