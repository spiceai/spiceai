# Metrics Naming

## TL;DR

**Metric Naming Guide**: Prioritize Developer Experience (DX) with intuitive, readable names that follow consistent conventions. Start with a domain prefix, use snake_case, avoid plurals in names (except counters), include units where relevant, and use labels for variations. Align with Prometheus and OpenTelemetry standards, and adhere to UCUM for units.

## Definitions

- Metric: is a measurement used to track the state and behavior of a system component. Metrics represent the current status (e.g., readiness or availability) or describe specific operations (e.g., request counts, cache hits, latencies, failures) to provide insight into system health, performance, and workload.
- Metric Name: The identifier for the metric, typically composed of a domain or namespace prefix followed by the metric's specific name.
  - Metric domain or namespace:  refers to a logical grouping or category that organizes related metrics. It typically indicates the component, system, or protocol that the metric belongs to and helps avoid naming conflicts and provides context for interpreting the metric.
- Label (or Dimension): Key-value pairs that provide additional context to a metric.
- Counter-Based Metrics: Metrics that represent a cumulative count of events or occurrences.
- Gauge-Based Metrics: Counter-Based Metrics that can increase or decrease over time, unlike regular counters which only increase. They are used for values that fluctuate, such as current active connections or in-progress operations.
- Unit-Based Metrics: Metrics that represent a specific unit of measurement, like duration or data size.

## Metric Naming Template

- **Format**: `<domain>_<metric>_<unit>` (for most metrics) or `<domain>_<metric_plural>` (for counters)
  - **Example**: `flight_request_duration_ms` or `results_cache_hits`
  - **Components**:
    - **Domain**: Functional area, logical group or protocol, e.g., `flight`, `results_cache`
    - **Metric**: Specific metric name, e.g., `request_duration`, `hits`
    - **Unit**: Unit of measurement, e.g., `ms` for milliseconds, using UCUM standard units where applicable. 

- Note: <metric_plural> includes the `_count` suffix when used for Gauges to record discrete instances of countable items, e.g. `db_client_active_connections_count`.

## Principles

These principles establish a coherent structure for naming metrics, ensuring metrics are logically organized, intuitive, and easy to analyze.

1. **Developer Experience (DX)**: Developer experience is the highest priority in metrics naming. Names should be intuitive, easy to read, and have consistent style. The names should make it simple for developers to understand and use metrics effectively.
   - **Why**: A developer-friendly naming scheme improves onboarding, reduces the cognitive load in understanding metrics, and fosters efficient debugging and monitoring. Well-designed naming conventions help developers immediately grasp the purpose of a metric without needing extensive documentation, accelerating development cycles and enhancing collaboration.

2. **Clarity over Concision**: Prioritize clarity in naming. Avoid abbreviations or verbosity that might obscure a metric’s purpose, aiming for names that are as clear as possible while remaining concise.
   - **Why**: Clear names reduce errors in interpretation and make metrics easier to use. 

3. **Consistency**: Follow a uniform structure across protocols (HTTP, gRPC, etc.) and metric types (Counter, Histogram, etc). Consistent naming makes it easier to organize, aggregate, and compare metrics.
   - **Why**: Consistency creates predictability, reducing the overhead required to parse metric names across different domains and enabling metrics to be used more effectively for cross-service analysis.

4. **Avoid Redundancy**: Avoid duplicating information in both metric names and labels. Include specific data either in the metric name or as a label, not both.
   - **Why**: Redundant information complicates metrics by adding clutter without analytical benefit. Simplified metrics improve readability and efficiency, aligning with the Developer Experience principle by reducing unnecessary complexity.

5. **Essential Data Only**: Ensure metrics capture necessary information for operational insights without excess detail. Use dimensions (labels) rather than new metrics for context-specific variations where feasible.
   - **Why**: Focusing on essential data makes metrics easier to consume and manage, ensuring they remain lightweight and performant, which contributes to a more streamlined developer experience.

6. **Adherence to Standards**: Align with established standards, prioritizing Prometheus as the primary standard while incorporating OpenTelemetry (OTEL) where relevant. If conflicts arise, prefer Prometheus conventions as they are more widely adopted. Additionally, adhere to the Unified Code for Units of Measure (UCUM) for unit naming where applicable, ensuring a consistent and scientifically recognized format.
   - **Why**: Adhering to industry standards facilitates interoperability with tools and ensures a consistent, predictable format across systems, supporting ease of integration. UCUM adherence guarantees unit naming is scientifically recognized and maintains accuracy across all metrics contexts, reinforcing Developer Experience by ensuring predictable and universally understood units.

7. **Avoid Undefined Nouns**: Ensure metric names are based on established terminology and well-understood concepts. Avoid creating new or ambiguous terms (e.g., "run") that lack a clear, universally accepted meaning. Use existing standards and consistent naming conventions to ensure that metrics are recognizable, intuitive, and easy to interpret.
   - **Why**: Undefined or ambiguous terms can lead to confusion, misinterpretation, and inconsistent usage. By sticking to established terminology, it is ensured that metrics are clear, meaningful, and aligned with common practices, making them easier to understand and use.

8. **Minimize Overhead**: Keep the performance impact of metric storage and querying low by minimizing high-cardinality dimensions and avoiding unnecessary detail.
   - **Why**: Reducing overhead ensures that the metrics remain cost-effective and performant, supporting sustainable monitoring practices. This aligns with Developer Experience by reducing delays and ensuring that system health can be monitored efficiently.

## Guidelines

The following guidelines apply the above principles, focusing on Developer Experience, clarity, and standard alignment, along with consistency and low overhead.

1. **Use Labels for Dimensions**: Represent variations (e.g., HTTP method, response status) as labels instead of embedding them in metric names.
   - **Why**: This improves metric flexibility and reusability, making it easier to filter and aggregate data, aligning with the Developer Experience priority and avoiding redundancy.

2. **Avoid High-Cardinality Attributes**: Avoid using high-cardinality labels (e.g., user IDs, request IDs) as they can significantly impact storage, query performance, and resource usage.
   - **Why**: Managing cardinality helps control resource usage and maintain cost-effective metrics, aligning with the Developer Experience priority by reducing performance bottlenecks.

3. **Use lowercase snake_case for Naming**: Follow `snake_case` for metric names, aligning with widely adopted conventions in Prometheus.
   - **Why**: Adhering to common casing conventions improves readability, reduces errors, and increases developer familiarity, which enhances Developer Experience.

4. **Non-Pluralized Naming**: Avoid plural forms in domains and metric names for uniformity, unless the value being recorded represents discrete instances of a [countable quantity](https://wikipedia.org/wiki/Count_noun). Generally, the name is pluralized only if the unit of the metric in question is a non-unit, e.g.,  `flight_requests`, `results_cache_hits`
   - **Why**: Singular terms are simpler and prevent naming inconsistencies, improving readability and maintaining consistency across metrics.

5. **Begin with a Domain Prefix**: Start each metric with a domain prefix or namespace, especially for protocol-based metrics.
   - **Why**: Domain prefixes provide immediate context, allowing developers to quickly understand a metric’s scope and relevance, supporting both Clarity and Developer Experience.

6. **Avoid Redundancy in Naming**: Avoid repeating terms that are implied by the context or domain, making metric names shorter and more readable. Example: `results_cache_hits` instead of `results_cache_cache_hits`, `query_duration_ms` instead of `query_query_duration_ms`.
   - **Why**: Redundancy adds unnecessary complexity and reduces readability. Shorter, more focused names enhance understanding and ease of use (Developer Experience principle).

7. **Counter Naming Conventions**: Avoid `_total` suffix in counter names e.g., `flight_requests` instead of `flight_requests_total`.   - **Why**: Standardized counter naming aligns with Prometheus conventions, reducing confusion and supporting Developer Experience by avoiding unnecessary naming variation.

8. **Gauge Naming**: Use `_count` suffix and pluralization only when recording discrete instances of countable items, e.g., `db_client_active_connections_count`.
   - **Why**: Clearly distinguishing counter-based metrics improves interpretability, which enhances the Developer Experience by making data analysis more intuitive.

9. **Include Units in Name for Unit-Based Metrics**: Specify the unit (e.g., `_ms` for milliseconds, `_bytes` for data size) within metric names where applicable. Units should adhere to UCUM standards for consistent scientific notation where applicable. The `_bytes` suffix is chosen in favor of the UCUM-recommended `By` as it is more intuitive and readable, prioritizing the Developer Experience (DX) principle.
   - **Why**: Including UCUM-adherent units in names prevents confusion over metric scale, especially when integrating with external tools, and reinforces Clarity and Developer Experience by providing universally understood units.

10. **Use Default Units for Common Metrics**:
   - **Family**: **Unit**
   - Timestamps and Duration: `ms`
    - Data size: `bytes`
   - **Why**: Establishing default units ensures predictability, reduces potential scaling errors, and simplifies interpretation, aligning with Developer Experience by maintaining consistency.

## References and Standards Alignment

This naming proposal integrates OpenTelemetry, Prometheus, and UCUM standards, combining Prometheus’s extensive adoption in metrics collection with OpenTelemetry’s semantic conventions and UCUM’s scientific accuracy for unit naming.

- [Prometheus Naming Best Practices](https://prometheus.io/docs/practices/naming/)
- [OpenTelemetry Metric Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/)
- [OpenTelemetry Attribute Naming](https://opentelemetry.io/docs/specs/semconv/general/attribute-naming/)
- [UCUM Standard for Units](https://ucum.org/trac)

### Notable Differences

- **From OpenTelemetry**:
  - Uses underscores (`_`) for domain separation instead of dots.
  - Explicitly includes unit suffixes (e.g., `query_execution_duration_ms` instead of `query.operation.duration`).
- **From Prometheus**:
  - Does not use `_total` suffix for counters.
  - Default units favor milliseconds (`ms`) over Prometheus’s preferred base unit of seconds.
