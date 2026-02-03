# SQL Federation

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

SQL Federation: Query Any Data Source With One Interface

Most organizations have data in 5-15 different systems. PostgreSQL for transactions. Snowflake for analytics. S3 for the data lake. Salesforce for CRM. MongoDB for documents.

Traditionally, to analyze data across these sources, you build ETL pipelines. Extract, transform, load into a warehouse. Then query the warehouse.

SQL Federation takes a different approach: query the sources directly, through a unified SQL interface.

The ETL approach has real costs. Storage duplication runs 3-10x. Data freshness is hours to days stale. There's constant pipeline maintenance. Schema changes break pipelines. You're managing infrastructure that exists only because your query systems can't talk to multiple sources.

Federation works differently. PostgreSQL, Snowflake, and S3 connect to a federation engine. You write one SQL query. The engine pushes computation to the sources and returns only matching results.

The key mechanism is query push-down. Without push-down, a query like SELECT FROM orders WHERE date greater than 2024-01-01 fetches ALL orders from PostgreSQL, then filters locally. Millions of rows cross the network. With push-down, the WHERE clause goes to PostgreSQL, PostgreSQL filters using its indexes, and only matching rows cross the network.

Push-down extends beyond filters. Projections push down (SELECT specific columns). Aggregations push down (SUM, COUNT, AVG computed at source). Joins push down when both tables are on the same source. Limits push down (LIMIT 100 applied at source).

The evolution of federation technology: 1990s data warehouses copied everything centrally—simple but stale. 2000s federated databases queried remote sources but pulled all data locally for processing—poor performance. 2010s data virtualization added caching and smarter planning—expensive, often became another silo. 2020s push-down federation pushes computation to sources, with only results crossing the network, leveraging each source's native optimization.

Tradeoffs to understand: Federation queries hit your source systems—they need capacity. Query latency includes network round-trips. Cross-source joins can't use source-side optimization. Caching decisions add complexity.

When federation works well: Real-time requirements where you can't wait for ETL. Schema volatility where sources change frequently. Exploratory queries where you don't know what you need yet. Cost sensitivity where you don't want to store data twice.

When ETL is still right: Heavy analytical workloads with aggregations across billions of rows. Strict latency requirements where pre-computed beats live queries. Complex transformations with business logic too complex for SQL.

From experience: I've built hundreds of ETL pipelines. Most of them existed because our query systems couldn't talk to multiple sources—not because ETL was the best architecture. Federation eliminates that constraint.

---

## X (5 posts, 280 characters each)

Post 1:
SQL Federation: query multiple data sources with one SQL interface. PostgreSQL, Snowflake, S3, Salesforce—one query, results from everywhere. No ETL pipelines, no data duplication, no staleness.

Post 2:
The key mechanism: push-down. Without it, fetch all rows then filter locally. With it, WHERE clause goes to source, source filters with its indexes, only matching rows cross the network. Huge difference.

Post 3:
Push-down extends to projections (SELECT columns), aggregations (SUM at source), joins (same-source), limits (LIMIT 100 at source). The source systems do the heavy lifting. Federation engine orchestrates.

Post 4:
Evolution: 90s warehouses (copy everything, stale). 00s federation (pull all data, slow). 10s virtualization (caching, expensive). 20s push-down (computation at source, only results travel).

Post 5:
When to federate: real-time needs, schema volatility, exploratory queries, cost sensitivity. When to ETL: billions of rows, strict latency, complex transforms. Most ETL exists because query systems couldn't talk to sources.
