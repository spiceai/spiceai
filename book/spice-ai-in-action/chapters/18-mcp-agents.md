# 18. MCP and bounded agent workflows

An agent needs tools that connect its reasoning to the environment. Model Context Protocol provides a standardized way to discover and call tools, resources, and related capabilities. Spice can participate as a server exposing capabilities and as a client or gateway connecting external tools. These are different directions of trust.

## 18.1 Draw the direction of each connection

When an external agent connects to Spice, Spice is a server. It must authenticate the caller, expose an appropriate tool surface, validate requests, and enforce resource boundaries. When Spice connects to an external MCP service, that service becomes a dependency whose outputs may influence a model.

The same deployment can do both. Draw each connection explicitly, including credentials, network access, and which process executes the tool. A tool reached through a local gateway can still call an external service and send data outside the environment.

![Figure 18.1. MCP server and gateway roles create separate trust boundaries.](figures/mcp.png)

## 18.2 Configure only the tools the task needs

A remote tool connection follows the supported configuration shape:

```yaml
# Integration fragment: illustrative internal service
tools:
  - name: policy_service
    from: mcp:https://policy.internal.example/mcp
    params:
      mcp_headers: 'X-API-KEY: ${ env:POLICY_TOOL_KEY }'
```

The hostname is illustrative. Verify the remote service's transport, authentication, and protocol version before deploying the connection.

For stdio tools, the runtime launches a local process. Its executable, arguments, package version, filesystem access, and environment become part of the deployment. Avoid fetching an unspecified package version on every startup. Install a reviewed artifact and give it only the access required by its tool contract.

Tool discovery is not approval to expose every discovered operation. The application should select a bounded toolset and review it when the remote server changes. A newly added destructive tool should not become callable merely because an existing endpoint returned it in a list.

## 18.3 Authenticate the MCP surface explicitly

Use the installed release's documentation for `/v1/mcp`, including its authentication and allowed-host requirements. The inspected source and docs include host checks and authenticated operation; exact startup requirements can differ from older recipes. A local-only URL does not make authentication irrelevant when tools can access sensitive data.

API-key authentication is a caller identity mechanism, not a row-level tenant policy. A shared service key identifies the application, so the application still needs to enforce the end user's scope. If multiple services have different capabilities, use distinct identities and explicit authorization boundaries.

When a proxy fronts MCP, preserve the intended host, authorization, and transport behavior. Test with the same proxy and path that production clients use. A direct localhost success does not validate an ingress configuration.

## 18.4 Design tools around business operations

Northstar's assistant needs operations such as `lookup_order`, `search_policies`, and `summarize_sales`. Each can have a typed input and a bounded response. It does not need a general shell or an unrestricted SQL execution tool to answer a return-policy question.

For `lookup_order`, the server derives tenant and user scope from the request context, validates the order ID, runs a fixed parameterized query, and returns only relevant fields. For `search_policies`, the server applies the authorized corpus boundary before passing text to any external reranker or generator.

A tool description should explain what “not found” means and which errors are retryable. Returning an empty object for both permission denial and source outage hides important differences. The agent can give a clearer answer when the tool returns a stable, structured error category.

## 18.5 Bound the agent loop

An agent loop receives a user request, calls a model, validates requested tool calls, executes permitted operations, returns results to the model, and eventually emits an answer. Bound the number of iterations, total tool calls, concurrent calls, total output size, and elapsed time.

Make termination explicit. A model that keeps searching for evidence after the available corpus has been exhausted should reach an “insufficient evidence” outcome rather than an unlimited loop. Retain the tool trace so a reviewer can see whether the agent used the right evidence.

Parallel tool calls are appropriate only when independent and allowed by the resource budget. An order lookup and a policy search may run independently after authorization. A refund submission must wait for eligibility checks and any required user action; it is not an independent read that can be speculatively issued.

## 18.6 Tool output is data

An external tool can return incorrect information, malformed content, or text that attempts to redirect the model's behavior. Treat its content as evidence with provenance, not as instructions that can override the application's policy.

Validate tool responses before using them. Check sizes, schema, identifiers, and expected origin. Keep sensitive fields out of model context unless they are required and allowed. Do not execute a command or follow a URL merely because a retrieved document tells the assistant to do so.

For source citations, map a verified document identity to a known route. A malicious passage can contain a lookalike link. The model's fluent citation formatting does not verify that destination.

## 18.7 Memory needs a lifecycle

Conversation or agent memory can improve continuity, but it is another retained data store. Define tenant scope, user scope, retention, deletion, and how stale facts are refreshed. A remembered shipping policy is not authoritative after the underlying policy changes.

Separate preferences from business facts. Remembering that a user prefers concise answers is different from remembering that an order is eligible for a refund. Requery current authoritative facts before making a consequential decision.

## 18.8 Test the workflow without the model first

Call each tool directly with valid, invalid, unauthorized, and oversized inputs. Test source outage and timeout behavior. Then connect the agent and verify that it selects the intended operations and handles their errors appropriately.

A model can mask a broken tool by producing a plausible answer without using it. For questions that require an order lookup, assert that the trace contains the authorized lookup and that the final answer uses the returned order facts. Tool-use evaluation should inspect the trace, not only the final prose.

**Exercise.** Specify three tools for Northstar, including schemas, maximum response size, retry behavior, and scope. Add one tool you deliberately exclude and explain which unnecessary capability it would grant.

**Further reading.** See cookbook `mcp/` and `mcp-server/`, the [MCP feature documentation](https://spiceai.org/docs/features/large-language-models/mcp), and the protocol specification referenced by the deployed client and server versions.
