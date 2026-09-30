# Cookbook: TypeSafe Jev evaluate

Configure `from: typesafe:jev`, export `TYPESAFE_API_KEY`, then call `POST /v1/evaluate` with `state` + typed `questions`.

Chat is N/A — pointing `/v1/chat/completions` at a TypeSafe model returns a clear 400 error directing you to `/v1/evaluate`.

Full docs: [TypeSafe models](../features/models/typesafe.md).
