"""Cognee Registration Kafka Consumer.

Subscribes to the enriched-code-chunks topic and calls cognee.add() for each
chunk message, registering file content in Cognee's metadata tables (datasets,
data, dataset_data) so that cognee.search() can discover these files.

Consumer group: cognee-registration-group (independent from other consumers).

Design goals:
- Decoupled from the preprocessor — no cognee dependency in the preprocessor
- Fails safely — cognee.add() errors are logged and do not block the pipeline
- Independent — can be enabled/disabled without affecting other consumers
"""
