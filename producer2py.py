from kafka import KafkaProducer
import json
import time
# from confluent_avro import AvroKeyValueSerde, SchemaRegistry
# from confluent_avro.schema_registry import HTTPBasicAuth
#
#
# # from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer
# from confluent_kafka import SerializingProducer
# import uuid


KAFKA_TOPIC = "topic_0"




producer = KafkaProducer(bootstrap_servers=["pkc-3w22w.us-central1.gcp.confluent.cloud:9092"],
                       security_protocol="SASL_SSL",
                       sasl_mechanism = "PLAIN",
                       sasl_plain_username = "YV3NUGTBVJHGBO4E",
                       sasl_plain_password = "kkea74p6nYCdHbCGmkxaqBO0Oi/papeoQtdq9CzeJ/FtJ1Vg+z3I9lpIsaNRuzKa",
                       client_id="client_id_test_1")



''' Done'''
key = 'key'
value = 'value'
#Produce JSON Message
for x in range(1, 2000):
 producer.send(
    KAFKA_TOPIC,
    key=json.dumps(key+str(x)).encode('utf-8'),
    value=json.dumps(value+str(x)).encode('utf-8'),
 )
 time.sleep(0.025)
 ''' Done'''

