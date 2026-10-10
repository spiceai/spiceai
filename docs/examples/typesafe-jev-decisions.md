# Cookbook: TypeSafe Jev decisions

Configure `from: typesafe:jev` and export `TYPESAFE_API_KEY`. Then ask typed questions in SQL with `ai_if`, `ai_probability`, `ai_classify`, `ai_score` and `ai_decide`, or call `POST /v1/decisions` with an OpenAI SDK.

Chat is N/A: pointing `/v1/chat/completions` at a TypeSafe model returns a 400 error directing you to `/v1/decisions`.

Full docs: [Decisions](../features/models/decisions.md) and [TypeSafe models](../features/models/typesafe.md).
