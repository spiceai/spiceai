# Preface {.unnumbered}

An application rarely has the luxury of keeping every fact it needs in one database. Orders live in an operational system. Product attributes arrive as files. Policies live in documents. Historical data sits in object storage. An AI assistant must reason over several of these sources, while a dashboard must answer quickly enough that people keep using it. The engineering problem is not simply to connect everything. It is to preserve meaning while deciding where work happens, how old the answers may be, and who may see them.

Spice.ai gives that problem a concrete deployment unit: a runtime that brings SQL query, data acceleration, search, and model inference close to an application. This book teaches you to build with that runtime and to take responsibility for the system around it. You will begin with five small files and finish with the design of an operational analytics service and a grounded support assistant. Along the way, you will inspect query plans, choose refresh strategies, reason about replication, evaluate retrieval, and practice recovery.

The title *Spice.ai in Action* describes the method. A capability becomes useful when you can configure it, observe it, explain its tradeoffs, and decide whether it meets your requirements. Configuration snippets alone do not establish that. Each major topic therefore connects a working example or an explicit integration procedure to a design decision.

## Who this book is for

The primary readers are software and data engineers who know basic SQL and have used a command line. You need not know Rust, DataFusion, Arrow, or vector search. The early chapters introduce these ideas at the level needed to make application decisions. Platform engineers can begin with the architecture chapter, run the starter project, and then concentrate on replication, deployment, observability, and recovery. AI application engineers should still work through the SQL correctness chapter: a fluent explanation of an incorrect aggregation is an incorrect answer.

This is an engineering book, not a catalog of vendor benchmarks. It does not promise a latency, a freshness bound, a compression ratio, or a cost reduction for your workload. It shows how to measure those properties and how to preserve the evidence that supports a decision. The small fixture is for understanding and verification, not performance comparison.

## The running project: Northstar

Northstar is a fictional retailer operating two tenant environments, `north` and `south`. Its application needs a daily sales view, a way to investigate orders, and a support assistant that cites the correct tenant's policies. All people, organizations, orders, and policies in the fixture are invented. The small data is deliberately awkward: one order has no customer, a paid order has a zero value, a customer has never ordered, orders contain several line items, and returns occur after purchases.

These are ordinary conditions, not exotic edge cases. They let us expose mistakes that disappear in perfectly rectangular sample data. The same fixture follows us from federated files to local acceleration, SQL APIs, and full-text retrieval. External integrations replace a source or add a service without changing the business questions.

## How to read and run the book

Read Chapters 1–4 in order. They establish the vocabulary, working directory, dataset names, and invariants used throughout. Chapters 5–12 explain the data plane: federation, connectors, lakehouses, acceleration, freshness, CDC, and Cayenne. Chapters 13–18 build the application and AI plane. Chapters 19–24 cover testing and production. Chapters 25 and 26 assemble the work into complete designs. Chapters 27–29 extend the discussion with deployment patterns, Cloud and BI integration, and engineering foundations drawn from the blog archive. Chapters 30–36 form the Enterprise section: distributions, identity and Cedar policy, Kubernetes operations, distributed acceleration, snapshots and recovery, functions and inference, and a deployment capstone. Appendix G provides the Enterprise workbook.

The accompanying `companion/` directory contains the fixture, executable SQL checks, alternative Spicepods, and application examples. Run commands from that directory unless a listing says otherwise. Shell examples use a POSIX shell. PowerShell users can run them in WSL or adapt quoting and environment-variable syntax. Configuration fragments explicitly labeled “fragment” must be merged into the indicated existing component; they are not independent Spicepods.

A listing marked **Local lab** uses the supplied fixture. A listing marked **Integration lab** requires the named external service, credentials, permissions, and supported runtime build. A **Design exercise** asks you to create or evaluate an operational policy. An observed result is labeled as such. Predicted results for integrations are acceptance criteria, not fabricated execution transcripts.

## Versions and evidence

This edition was prepared on September 7, 2026, using the Spice source checkout at commit `16c436f7b8a76ce161a07c0288277ced0ada4a07`. Its workspace version is `2.3.0-unstable`. The local starter SQL was executed with an installed runtime reporting `v2.1.1+models.metal`. Additional integration results, including the precise build used, are recorded in Appendix A and the accompanying `evidence/` directory. A development binary is not represented as a released version or as a build of a different commit.

The OSS documentation and cookbook checkouts were consulted at commits `e30ef3d3dd84f7ba32c1eb32fc7f7f5d5dc6f375` and `a76a26add6545c7edf986791e23966eafcb09a7b`, respectively. The website blog archive was incorporated at commit `f1e0f9be59753b7e088a1bba6d57dc209650a6bd`, with all 50 posts classified in Appendix F. The Enterprise section additionally uses the supplied `~/dev/ent`, `~/dev/spice-k8s-operator`, and `~/dev/docs` checkouts; their exact identities are recorded in Appendix F. Public documentation was also checked during preparation. Documentation versions, source commits, and executable versions are distinct artifacts. Keep all three in your own deployment record.

The examples use `version: v1` for the Spicepod document format accepted by the tested runtime. That value is not the installed Spice release number. A newer CLI may generate another supported format. Use the schema and documentation matching your binary instead of changing that field to a runtime version.

Where a feature evolves quickly—especially replication recovery, search, clustered execution, and model gateways—the chapter presents a durable engineering model and identifies the configuration to verify. It does not infer a release guarantee from a newer source file. The source register at the end of the book gives readers a path back to the authoritative material.

## Conventions

`Monospace` identifies commands, SQL, paths, configuration keys, and code. Currency is stored in integer cents in the fixture. Dates and timestamps represent UTC unless the text explicitly introduces a business timezone. Output ordering is meaningful only when the query includes `ORDER BY`. Ellipses inside prose indicate omitted discussion; executable companion files contain no omitted implementation.

The word *source* means the system from which a dataset obtains data. *Accelerator* means the configured local or attached representation used for accelerated access. *Result cache* means stored results of particular queries. *Freshness* means how far the data visible to a request trails the business state it is expected to represent. These distinctions recur because confusing them produces architectural mistakes.

## Using the material

The manuscript is an original technical work. It is not affiliated with or published by O'Reilly Media or Packt. Product and company names identify their respective products; they do not imply endorsement. Upstream source and recipe references retain their own licenses. The source register identifies the materials used for technical grounding.

Start by running the project, including the deliberately wrong queries. Keep the outputs. By the time you reach the production chapters, those few rows will have become a compact contract for what your larger system must continue to mean.
