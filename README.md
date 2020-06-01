## Kewpie

Kewpie is a task queue abstraction. It supports pluggable backends with a single common interface.

The format of tasks stored in backends is designed to be simple to write and consume, making it easy to write kewpie libraries in many languages.

The currently supported backends are:
1. Amazon SQS
2. PostgreSQL
3. Memory (designed for testing only)
4. Google PubSub (in progress, not yet ready for production)
