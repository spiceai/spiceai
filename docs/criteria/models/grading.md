# Models Grading

This criteria defines a set of grades, from Ungraded to Grade A, to classify a models capabilities when used with the Spice Runtime.

- **Ungraded:** Represents the lowest grade a model can receive against the Spice Runtime. This grade means the model was tested with the Spice Runtime, but was unable to meet the requirements for at least Grade C.
- **Grade C:** Represents models with the lowest certification for use with the Spice Runtime, supporting basic chat and AI gateway use cases.
- **Grade B:** Represents models certified for use with the Spice Runtime which support more advanced use cases, like tool use (collecting data from connected sources) and text-to-SQL (NSQL).
- **Grade A:** Represents the most advanced model capabilities certified for use with the Spice Runtime, suitable for any use case that requires reasoning, structured outputs, or a high success rate in tool use and text-to-SQL (NSQL).

## Grading Definitions

### Ungraded

Represents a model which has been tested with the Spice Runtime, but cannot be classified as Grade C.

### Grade C

Represents models with the lowest certification for use with the Spice Runtime, supporting the simplest chat and AI gateway use cases.

- **Capabilities:**
  - The model supports chat completions.
  - The model supports input tokens larger than or equal to 8K tokens.
  - The model supports output tokens larger than or equal to 4K tokens.
  - The model is not required to support using tools via the Spice Runtime.
- **Grading:**
  - More than 50% of the time, the model produces valid SQL when using text-to-SQL (NSQL) queries.
  - More than 50% of the time, the model does not enter a recursion loop through any means (tool use, chat completions, etc).

#### Grade-B

Represents models certified for use with the Spice Runtime which support more advanced use cases, like tool use (collecting data from connected sources) and text-to-SQL (NSQL).

- **Capabilities:**
  - The model supports tools and tool use, and can call tools via the Spice Runtime.
  - The model successfully recursively calls tools via the Spice Runtime, when applicable.
  - The model supports chat completions.
  - The model supports input tokens larger than or equal to 32K tokens.
  - The model supports output tokens larger than or equal to 8K tokens.
  - The model supports structured outputs.
  - The model supports streaming responses.
- **Grading:**
  - More than 75% of the time correctly outputs structured output when it is requested.
  - More than 75% of the time, the model does not enter a recursion loop through any means (tool use, chat completions, etc).
  - More than 75% of the time, the model produces valid SQL when using text-to-SQL (NSQL) queries.
  - More than 75% of the time, the model accurately references documentation and information collected through tool use, including providing accurate citations to connected sources (including SQL tables).

#### Grade-A

Represents the most advanced model capabilities certified for use with the Spice Runtime, suitable for any use case that requires reasoning, structured outputs, or a high success rate in tool use and text-to-SQL (NSQL).

- **Capabilities:**
  - The model supports tools and tool use, and can call tools via the Spice Runtime.
  - The model successfully recursively calls tools via the Spice Runtime, when applicable.
  - The model supports chat completions.
  - The model supports input tokens larger than or equal to 128K tokens.
  - The model supports output tokens larger than or equal to 16K tokens.
  - The model supports reasoning capabilities.
  - The model supports structured outputs.
  - The model supports streaming responses.
- **Grading:**
  - More than 90% of the time correctly outputs structured output when it is requested.
  - More than 90% of the time, the model does not enter a recursion loop through any means (tool use, chat completions, etc).
  - More than 90% of the time, the model produces valid SQL when using text-to-SQL (NSQL) queries.
  - More than 90% of the time, the model accurately references documentation and information collected through tool use, including providing accurate citations to connected sources (including SQL tables).
