# Not in used yet, but can be used to register schema in schema registry
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

schema_registry_conf = {'url': 'http://localhost:8081'}

client = SchemaRegistryClient(schema_registry_conf)

with open('schemas/event_v1.avsc') as f:
    schema_str = f.read()

schema = Schema(schema_str, 'AVRO')

schema_id = client.register_schema(
    "ecommerce-events-value",
    schema
)

print(f"Registered schema ID: {schema_id}")