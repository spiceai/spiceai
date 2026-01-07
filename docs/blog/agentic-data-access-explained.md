# Agentic Data Access: Beyond RAG to Autonomous Data Agents

*How to build AI agents that autonomously compose SQL, search, and inference to answer complex questions.*

> **Part of the Spice Use Case Series**: This article is one of our deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [The Evolution from RAG to Agents](#the-evolution-from-rag-to-agents)
3. [What is Agentic Data Access?](#what-is-agentic-data-access)
4. [Agent Architecture Patterns](#agent-architecture-patterns)
5. [Building Data Agents with Spice](#building-data-agents-with-spice)
6. [Guardrails and Safety](#guardrails-and-safety)
7. [Observability and Debugging](#observability-and-debugging)
8. [Real-World Use Cases](#real-world-use-cases)
9. [Getting Started](#getting-started)
10. [Conclusion](#conclusion)

---

## Introduction

RAG (Retrieval-Augmented Generation) revolutionized how AI applications access data. But RAG has a fundamental limitation: it's a single-shot retrieval followed by generation. Real-world questions often require **multi-step reasoning**, **iterative refinement**, and **tool composition**.

**Agentic Data Access** goes beyond RAG by giving AI agents the ability to:

- Decompose complex questions into sub-queries
- Choose the right tool (SQL, search, API) for each step
- Iterate based on intermediate results
- Compose multiple data sources autonomously

This article explores how to build autonomous data agents that reason over your data infrastructure.

---

## The Evolution from RAG to Agents

### Generation 1: Simple RAG

```text
User Question → Embed → Vector Search → Top-K Results → LLM → Answer

Limitations:
- Single retrieval attempt
- No reasoning about what to retrieve
- Can't combine structured and unstructured data
```

### Generation 2: Advanced RAG

```text
User Question → Query Rewriting → Hybrid Search → Reranking → LLM → Answer

Improvements:
- Better retrieval through query transformation
- Multiple search modalities
- Still fundamentally single-shot
```

### Generation 3: Agentic Data Access

```text
User Question
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Agent Reasoning Loop                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Think: "I need to find customer orders, then calculate totals" │
│     │                                                            │
│     ▼                                                            │
│  Action: sql_query("SELECT * FROM orders WHERE customer=...")   │
│     │                                                            │
│     ▼                                                            │
│  Observe: [order_1, order_2, order_3]                           │
│     │                                                            │
│     ▼                                                            │
│  Think: "Now I need product details for these orders"           │
│     │                                                            │
│     ▼                                                            │
│  Action: sql_query("SELECT * FROM products WHERE id IN (...)")  │
│     │                                                            │
│     ▼                                                            │
│  Observe: [product_a, product_b]                                │
│     │                                                            │
│     ▼                                                            │
│  Think: "I have all the data, now I can answer"                 │
│     │                                                            │
│     ▼                                                            │
│  Final Answer: "Your orders total $1,234..."                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## What is Agentic Data Access?

Agentic Data Access gives AI agents **autonomous control** over data retrieval and analysis:

### Core Capabilities

| Capability               | Description                                 | Example                                 |
| ------------------------ | ------------------------------------------- | --------------------------------------- |
| **Tool Selection**       | Choose SQL, search, or API based on query   | "Find similar products" → vector_search |
| **Query Composition**    | Build complex queries from natural language | Multi-table JOINs with filters          |
| **Iterative Refinement** | Adjust approach based on results            | "No results, try broader search"        |
| **Result Synthesis**     | Combine data from multiple sources          | SQL + search + external API             |

### The Agent Loop

```text
┌─────────────────────────────────────────────────────────────────┐
│                     ReAct Agent Pattern                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                │
│  │  REASON  │ ──▶ │   ACT    │ ──▶ │ OBSERVE  │                │
│  │          │     │          │     │          │                │
│  │ "What do │     │ Execute  │     │ Process  │                │
│  │  I need?"│     │ tool     │     │ results  │                │
│  └──────────┘     └──────────┘     └────┬─────┘                │
│       ▲                                  │                      │
│       │                                  │                      │
│       └──────────────────────────────────┘                      │
│                    Loop until done                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Agent Architecture Patterns

### Pattern 1: SQL Agent

An agent that translates natural language to SQL queries:

```yaml
# spicepod.yaml
models:
  - name: sql_agent
    from: openai
    params:
      model: gpt-4-turbo
      system_prompt: |
        You are a SQL expert. Given a question and database schema,
        generate SQL queries to answer the question.
        
        Available tables:
        - customers (id, name, email, created_at)
        - orders (id, customer_id, total, status, created_at)
        - products (id, name, price, category)
        
        Always use parameterized queries for safety.

datasets:
  - name: customers
    from: postgres:crm.customers
    
  - name: orders
    from: postgres:sales.orders
    
  - name: products
    from: postgres:catalog.products
```

**Agent workflow**:

```text
User: "Which customers haven't ordered in 6 months but were previously active?"

Agent Reasoning:
1. Need to find customers with orders > 6 months ago
2. Exclude customers with recent orders
3. Define "active" as having made multiple orders

Generated SQL:
SELECT c.name, c.email, MAX(o.created_at) as last_order
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.id
HAVING MAX(o.created_at) < NOW() - INTERVAL '6 months'
   AND COUNT(o.id) > 3
ORDER BY last_order DESC;
```

### Pattern 2: Hybrid Search Agent

An agent that combines vector, text, and SQL search:

```yaml
models:
  - name: search_agent
    from: anthropic
    params:
      model: claude-3-5-sonnet
      system_prompt: |
        You have access to three search tools:
        
        1. vector_search(table, query, limit) - Semantic similarity
        2. text_search(table, query, limit) - Keyword/BM25 matching
        3. sql_query(query) - Structured data filtering
        
        Choose the right tool(s) for each question.
        Combine results when multiple approaches help.

datasets:
  - name: knowledge_base
    from: s3://docs/
    embeddings:
      - column: content
        use: openai_embeddings
```

**Agent workflow**:

```text
User: "Find documentation about authentication errors for enterprise customers"

Agent Reasoning:
1. "authentication errors" is a semantic concept → vector_search
2. "enterprise" is a specific term → text_search for precision
3. Combine results with RRF fusion

Actions:
1. vector_search(knowledge_base, 'authentication errors troubleshooting', 20)
2. text_search(knowledge_base, 'enterprise authentication', 20)
3. Fuse and rerank results
```

### Pattern 3: Multi-Step Analysis Agent

An agent that performs complex analytical workflows:

```text
User: "Analyze our Q4 sales performance compared to Q3, 
       identify underperforming products, and suggest actions"

Agent Execution:
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Gather Q4 sales data                                    │
│ → sql_query("SELECT product_id, SUM(revenue) FROM sales        │
│              WHERE quarter = 'Q4' GROUP BY product_id")         │
├─────────────────────────────────────────────────────────────────┤
│ Step 2: Gather Q3 sales data                                    │
│ → sql_query("SELECT product_id, SUM(revenue) FROM sales        │
│              WHERE quarter = 'Q3' GROUP BY product_id")         │
├─────────────────────────────────────────────────────────────────┤
│ Step 3: Calculate growth rates                                  │
│ → Compute (Q4 - Q3) / Q3 for each product                      │
├─────────────────────────────────────────────────────────────────┤
│ Step 4: Identify underperformers (growth < -10%)               │
│ → Filter products with negative growth                          │
├─────────────────────────────────────────────────────────────────┤
│ Step 5: Search for context on underperforming products         │
│ → vector_search(product_feedback, 'issues complaints', 10)     │
├─────────────────────────────────────────────────────────────────┤
│ Step 6: Synthesize findings and recommendations                 │
│ → Generate executive summary with actionable insights           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Building Data Agents with Spice

### Tool Definition

Define the tools available to your agent:

```sql
-- Tool 1: SQL Query Execution
-- Agent can execute arbitrary SELECT queries against allowed datasets

-- Tool 2: Vector Search
SELECT * FROM vector_search(knowledge_base, $query, $limit);

-- Tool 3: Text Search  
SELECT * FROM text_search(documents, $query, $limit);

-- Tool 4: AI Inference
SELECT ai($prompt, 'gpt-4') as response;
```

### Agent Orchestration

```python
# Example agent implementation using Spice
import spice
from openai import OpenAI

client = OpenAI()
spice_client = spice.Client()

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "sql_query",
            "description": "Execute a SQL query against the data warehouse",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL SELECT query"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function", 
        "function": {
            "name": "vector_search",
            "description": "Semantic similarity search",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "query": {"type": "string"},
                    "limit": {"type": "integer", "default": 10}
                },
                "required": ["table", "query"]
            }
        }
    }
]

def execute_tool(name: str, args: dict):
    if name == "sql_query":
        return spice_client.query(args["query"])
    elif name == "vector_search":
        sql = f"SELECT * FROM vector_search({args['table']}, '{args['query']}', {args.get('limit', 10)})"
        return spice_client.query(sql)

def run_agent(user_question: str, max_iterations: int = 5):
    messages = [{"role": "user", "content": user_question}]
    
    for i in range(max_iterations):
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=messages,
            tools=TOOLS
        )
        
        message = response.choices[0].message
        
        if message.tool_calls:
            # Execute tool calls
            for tool_call in message.tool_calls:
                result = execute_tool(
                    tool_call.function.name,
                    json.loads(tool_call.function.arguments)
                )
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": str(result)
                })
        else:
            # Agent is done reasoning
            return message.content
    
    return "Max iterations reached"
```

---

## Guardrails and Safety

### Query Validation

Prevent dangerous operations:

```yaml
# spicepod.yaml
runtime:
  query_validation:
    enabled: true
    rules:
      - deny: "DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE"
        message: "Only SELECT queries are allowed"
      - deny: "information_schema|pg_catalog"
        message: "System tables are not accessible"
      - max_rows: 10000
        message: "Query would return too many rows"
```

### Data Scoping

Limit what data agents can access:

```yaml
datasets:
  # Agent can only see this customer's data
  - name: customer_orders
    from: postgres:orders
    params:
      filter: "customer_id = '${session.customer_id}'"
    
  # Read-only access to products
  - name: products
    from: postgres:products
    access: read
```

### Rate Limiting

Prevent runaway agents:

```yaml
runtime:
  rate_limiting:
    enabled: true
    requests_per_minute: 60
    tokens_per_minute: 100000
```

### Cost Controls

Set spending limits:

```yaml
models:
  - name: gpt-4
    from: openai
    params:
      max_tokens_per_request: 4096
      max_requests_per_session: 20
```

---

## Observability and Debugging

### Tracing Agent Execution

```text
┌─────────────────────────────────────────────────────────────────┐
│ Trace ID: abc-123                                                │
│ User: "What were our top products last quarter?"                │
├─────────────────────────────────────────────────────────────────┤
│ Step 1 [0ms - 234ms]                                            │
│   Action: sql_query                                              │
│   Query: SELECT product_id, SUM(revenue)...                     │
│   Rows: 156                                                      │
│   Tokens: 89 input, 0 output                                    │
├─────────────────────────────────────────────────────────────────┤
│ Step 2 [234ms - 567ms]                                          │
│   Action: sql_query                                              │
│   Query: SELECT name, category FROM products...                 │
│   Rows: 156                                                      │
│   Tokens: 234 input, 0 output                                   │
├─────────────────────────────────────────────────────────────────┤
│ Step 3 [567ms - 1203ms]                                         │
│   Action: generate_response                                      │
│   Tokens: 892 input, 456 output                                 │
│   Cost: $0.0234                                                  │
├─────────────────────────────────────────────────────────────────┤
│ Total: 1203ms | 3 steps | $0.0234                               │
└─────────────────────────────────────────────────────────────────┘
```

### Metrics to Monitor

| Metric              | Description                | Alert Threshold |
| ------------------- | -------------------------- | --------------- |
| **Steps per query** | Agent reasoning iterations | > 10 steps      |
| **Latency p99**     | End-to-end response time   | > 30s           |
| **Token usage**     | LLM tokens consumed        | > budget        |
| **Error rate**      | Failed tool executions     | > 5%            |
| **Cost per query**  | Total API spend            | > $1            |

---

## Real-World Use Cases

### Use Case 1: Customer Support Agent

```text
Customer: "I was charged twice for my order last week"

Agent Actions:
1. sql_query: Find customer's recent orders
2. sql_query: Find payment transactions for those orders
3. Identify: Duplicate charge on order #12345
4. vector_search: Find refund policy documentation
5. Generate: "I found a duplicate charge of $49.99 on order #12345. 
              I've initiated a refund which will appear in 3-5 days..."
```

### Use Case 2: Business Intelligence Agent

```text
Analyst: "Why did conversion rates drop in the EU last month?"

Agent Actions:
1. sql_query: Calculate EU conversion rates by week
2. sql_query: Compare to previous months
3. sql_query: Break down by country, device, traffic source
4. Identify: Germany mobile conversions dropped 40%
5. vector_search: Find any incidents or changes in German market
6. Generate: "EU conversion drop was driven by Germany mobile users.
              This coincides with the checkout redesign on Oct 15..."
```

### Use Case 3: Research Assistant

```text
Researcher: "Find papers about transformer architectures published 
             after 2022 that cite the original attention paper"

Agent Actions:
1. vector_search: Find papers semantically related to transformers
2. sql_query: Filter to publication_date > 2022
3. sql_query: Join with citations table, filter for attention paper
4. text_search: Refine with specific architecture terms
5. Generate: Ranked list of 15 relevant papers with summaries
```

---

## Getting Started

### Step 1: Configure Data Sources

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: data-agent

datasets:
  - name: orders
    from: postgres:sales.orders
    acceleration:
      enabled: true
      engine: duckdb
      
  - name: knowledge_base
    from: s3://company-docs/
    embeddings:
      - column: content
        use: openai_embeddings
```

### Step 2: Configure Models

```yaml
models:
  - name: agent_brain
    from: openai
    params:
      model: gpt-4-turbo
      openai_api_key: ${secrets:OPENAI_API_KEY}

embeddings:
  - name: openai_embeddings
    from: openai
    params:
      model: text-embedding-3-small
```

### Step 3: Build Your Agent

Use any agent framework (LangChain, CrewAI, AutoGen) with Spice as the data backend:

```python
from langchain.agents import create_sql_agent
from langchain_community.utilities import SpiceDBWrapper

# Spice provides the data layer
db = SpiceDBWrapper(connection_string="localhost:50051")

# Your favorite agent framework provides the reasoning
agent = create_sql_agent(llm=ChatOpenAI(), db=db, verbose=True)

agent.run("What are the trends in customer churn?")
```

---

## Conclusion

Agentic Data Access represents the next evolution of AI-powered data applications:

| Approach         | Capability           | Complexity |
| ---------------- | -------------------- | ---------- |
| **Simple RAG**   | Single retrieval     | Low        |
| **Advanced RAG** | Hybrid retrieval     | Medium     |
| **Agentic**      | Multi-step reasoning | High       |

Key takeaways:

- **Agents compose tools** rather than relying on single-shot retrieval
- **Guardrails are essential** for production safety
- **Observability enables debugging** complex agent behaviors
- **Spice provides the data substrate** that agents reason over

The future of data applications is autonomous agents that navigate your data infrastructure as fluently as a human analyst.

---

## Related Articles in This Series

- **[RAG (Retrieval-Augmented Generation)](rag-explained.md)**: Foundation for agent context retrieval
- **[LLM Inference](llm-inference-explained.md)**: SQL-native AI model access
- **[Hybrid SQL Search](hybrid-sql-search-explained.md)**: Multi-modal search for agents
- **[Secure AI Agents](secure-ai-agents-explained.md)**: Sandboxing and governance

---

## Further Reading

- [ReAct: Synergizing Reasoning and Acting](https://arxiv.org/abs/2210.03629)
- [LangChain Agents Documentation](https://python.langchain.com/docs/modules/agents/)
- [Spice AI Gateway Documentation](https://spiceai.org/docs/features/ai-gateway)
