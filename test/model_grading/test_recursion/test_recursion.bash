#!/bin/bash

make_request() {
    local content="$1"
    echo "Executing: $content\n"
    curl -L 'http://localhost:8090/v1/chat/completions' \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/json' \
        -d "{
            \"messages\": [
                {
                    \"content\": \"$content\",
                    \"role\": \"user\"
                }
            ],
            \"model\": \"openai_model\"
        }"
}

# Array of queries
queries=(
    "What datasets do you have access to?"
    "Perform a random sample from customer table and nation table?"
    "What is the schema of the lineitem table and orders table?"
    "Sample the top 10 rows from customer table based on the nation key."
    "Sample 10 distinct rows from region table."
    "Find part brand for part with key 3."
    "Find the name and account balance of suppliers whose account balance is above 9000."
    "Compare the difference in the schema of partsupp table and part table."
    "Perform 2 samples of distinct data on the custkey column of customer dataset, and compare the sample result."
    "Find the average supply cost for each part brand."
)

# Execute queries
for query in "${queries[@]}"; do
    make_request "$query"
done

# Run final SQL analysis
echo "Running SQL analysis..."
spice sql << EOF
select parent_span_id, count(*) 
from spice.runtime.task_history 
where parent_span_id in (
    select span_id from spice.runtime.task_history 
    where task = 'ai_chat' and parent_span_id is null
)
group by parent_span_id;
EOF