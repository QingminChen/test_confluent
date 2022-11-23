import json
import time
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth

# from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
import uuid

KAFKA_TOPIC = "topic_0"


# registry_client = SchemaRegistry("https://psrc-30dr2.us-central1.gcp.confluent.cloud",HTTPBasicAuth("testgcpusertwo@gmail.com", "2n*7db^f"),
#     headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
# )

# registry_client = SchemaRegistry("https://psrc-30dr2.us-central1.gcp.confluent.cloud",HTTPBasicAuth("testgcpusertwo@gmail.com", "2n*7db^f"))

# registry_client = SchemaRegistry('https://TBZ3QR4ZVVWC2HBM:Wr3lWLcPOmd7gZdTQBEro7FCBFODJW+qHikth1ESJ4Fsw/w14F2LlEcJ8T+qGYo6@psrc-30dr2.us-central1.gcp.confluent.cloud:443')

# https://{API KEY}:{URL-ENCODED SCHEMA REGISTRY SECRET KEY}@{SCHEMA REGISTRY URL}:443


# HTTPBasicAuth.auth.credentials.source='USER_INFO'
# HTTPBasicAuth.auth.user.info='TBZ3QR4ZVVWC2HBM:Wr3lWLcPOmd7gZdTQBEro7FCBFODJW+qHikth1ESJ4Fsw/w14F2LlEcJ8T+qGYo6'

# avro_serde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)
#
#
# avroSerde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)

name_schema = """
    {
        "namespace": "com.bunny.namespace",
        "name": "Name",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    }
"""

class Name(object):
    """
        Name stores the deserialized Avro record for the Kafka key.
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "id"]

    def __init__(self, name=None):
        self.name = name
        self.id = 123

    @staticmethod
    def dict_to_name(obj, ctx):
        return Name(obj['name'])

    @staticmethod
    def name_to_dict(name, ctx):
        return Name.to_dict(name)

    def to_dict(self):
        return dict(name=self.name)

count_schema = """
    {
        "namespace": "com.bunny.namespace",
        "name": "Count",
        "type": "record",
        "fields": [
            {"name": "count", "type": "int"}
        ]
    }
"""



class Count(object):
    """
        Count stores the deserialized Avro record for the Kafka value.
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["count", "id"]

    def __init__(self, count=None):
        self.count = count
        self.id = 123

    @staticmethod
    def dict_to_count(obj, ctx):
        return Count(obj['count'])

    @staticmethod
    def count_to_dict(count, ctx):
        return Count.to_dict(count)

    def to_dict(self):
        return dict(count=self.count)


schema_registry_conf = {
        'url': 'https://psrc-30dr2.us-central1.gcp.confluent.cloud',
        'basic.auth.user.info': 'UA43IMWADORY665N:gOJdkM8XrmtGQRQSGH2KMl7sAEsa1hEaKp1ZF2U1w7SiT2zFFbde2WEVgD/3zIoF'}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


name_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
                                          schema_str = name_schema,
                                          to_dict = Name.name_to_dict)
count_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
                                           schema_str =  count_schema,
                                           to_dict = Count.count_to_dict)


producer_conf = {}
producer_conf['bootstrap.servers']="pkc-3w22w.us-central1.gcp.confluent.cloud:9092"
producer_conf['security.protocol']='SASL_SSL'
producer_conf['sasl.mechanisms']='PLAIN'
# producer_conf['sasl.jaas.config']='org.apache.kafka.common.security.plain.PlainLoginModule'
# producer_conf['schema.registry.url']='https://psrc-30dr2.us-central1.gcp.confluent.cloud'
# producer_conf['basic.auth.user.info']='TBZ3QR4ZVVWC2HBM:Wr3lWLcPOmd7gZdTQBEro7FCBFODJW+qHikth1ESJ4Fsw/w14F2LlEcJ8T+qGYo6'
producer_conf['key.serializer']=name_avro_serializer
producer_conf['value.serializer']=count_avro_serializer
producer_conf['sasl.username']='EBKPHWLGDLAIHLQB'
producer_conf['sasl.password']='rANm+MY3eZqyshbT3p1GVQJpZKXhrpMp350eGVxmBpvT6bi4WOykPmbLPaOjWm2s'
# producer_conf['sasl.username']="testgcpusertwo@gmail.com"
# producer_conf['sasl.password']="2n*7db^f",
# producer_conf['enable.ssl.certificate.verification']=False
# conf.pop('basic.auth.credentials.source', None)

producer = SerializingProducer(producer_conf)



# records = [
#     {
#         "key": {
#             "num": i
#         },
#         "value": {
#             "Title": "title_"+str(i),
#             "num": i
#         }
#     } for i in range(0, 5000)
# ]


# #Produce Avro Message
# for x in range(1, 20):
#  producer.send(
#     KAFKA_TOPIC,
#     # key=string_serializer(str(uuid4())),
#     key = avroSerde.key.serialize({...},key_schema),
#     value=avroSerde.value.serialize({...}, value_schema)
#  )
#  time.sleep(0.025)

# f_k = open ('/Users/chenqingmin/Codes/Company_Workspace/test_confluent/test_list_key.json', "r")
# f_v = open ('/Users/chenqingmin/Codes/Company_Workspace/test_confluent/test_list_value.json', "r")
#
# key_schema = json.loads(f_k.read())
# value_schema = json.loads(f_v.read())


def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))

delivered_records = 0
for n in range(5000):
    name_object = Name()
    name_object.name = "alice"
    count_object = Count()
    count_object.count = n
    print("Producing Avro record: {}\t{}".format(name_object.name, count_object.count))
    producer.produce(topic=KAFKA_TOPIC, key=name_object, value=count_object,on_delivery=acked)
    producer.poll(0)
    time.sleep(0.025)

producer.flush()


# from flask import Flask
# app = Flask(__name__)
#
# @app.route("/")
# def hello_world():
#     return "<p>Hello, World!</p>"
#
#
# if __name__ == "__main__":
#     app.run(host="185.217.119.33")



response_json={
  "statusCode": 200,
  "headers": {
    "Transfer-Encoding": "chunked",
    "Vary": "Origin,Accept-Encoding",
    "X-NetworkStatistics": "0,525568,0,0,390466,0,226335",
    "X-SharePointHealthScore": "1",
    "X-MS-SPConnector": "1",
    "X-SP-SERVERSTATE": "ReadOnly=0",
    "DATASERVICEVERSION": "3.0",
    "SPClientServiceRequestDuration": "68",
    "X-DataBoundary": "None",
    "X-1DSCollectorUrl": "https://mobile.events.data.microsoft.com/OneCollector/1.0/",
    "X-AriaCollectorURL": "https://browser.pipe.aria.microsoft.com/Collector/3.0/",
    "SPRequestGuid": "96314ffe-b902-421f-9de7-30c97a77a6ad",
    "request-id": "96314ffe-b902-421f-9de7-30c97a77a6ad",
    "MS-CV": "/k8xlgK5H0Kd5zDJenemrQ.0",
    "Strict-Transport-Security": "max-age=31536000",
    "X-FRAME-OPTIONS": "SAMEORIGIN",
    "Content-Security-Policy": "frame-ancestors 'self' teams.microsoft.com *.teams.microsoft.com *.skype.com *.teams.microsoft.us local.teams.office.com *.powerapps.com *.yammer.com *.officeapps.live.com *.office.com *.stream.azure-test.net *.microsoftstream.com *.dynamics.com *.microsoft.com securebroker.sharepointonline.com;",
    "MicrosoftSharePointTeamServices": "16.0.0.23102",
    "X-Content-Type-Options": "nosniff",
    "X-MS-InvokeApp": "1; RequireReadOnly",
    "Timing-Allow-Origin": "*",
    "x-ms-apihub-cached-response": "true",
    "x-ms-apihub-obo": "false",
    "Cache-Control": "max-age=0, private",
    "Date": "Mon, 21 Nov 2022 08:51:16 GMT",
    "P3P": "CP=\"ALL IND DSP COR ADM CONo CUR CUSo IVAo IVDo PSA PSD TAI TELo OUR SAMo CNT COM INT NAV ONL PHY PRE PUR UNI\"",
    "X-AspNet-Version": "4.0.30319",
    "X-Powered-By": "ASP.NET",
    "Content-Type": "application/json; charset=utf-8",
    "Expires": "Sun, 06 Nov 2022 07:51:17 GMT",
    "Last-Modified": "Mon, 21 Nov 2022 08:51:17 GMT",
    "Content-Length": "1901"
  },
  "body": {
    "value": [
      {
        "@odata.etag": "\"5\"",
        "ItemInternalId": "3523",
        "ID": 3523,
        "Title": "111111",
        "EID": "aaaaaaaaaaa",
        "datekey": "111",
        "ApplicationNm": "111",
        "Portfolio": "111",
        "AIRID": "111",
        "dimension": "1",
        "metric": "111",
        "value": 1.0,
        "Modified": "2022-11-21T08:50:29Z",
        "Created": "2022-11-21T07:06:37Z",
        "Author": {
          "@odata.type": "#Microsoft.Azure.Connectors.SharePoint.SPListExpandedUser",
          "Claims": "i:0#.f|membership|xxxx@test.com",
          "DisplayName": "Bunny",
          "Email": "xxxx@test.com",
          "Picture": "https://ts.xxxx.com/sites/ExperienceMeasurements/_layouts/15/UserPhoto.aspx?Size=L&AccountName=xxxx@test.com",
          "Department": 'null',
          "JobTitle": "Custom Software Engineering Sr Analyst"
        },
        "Author#Claims": "i:0#.f|membership|xxxx@test.com",
        "Editor": {
          "@odata.type": "#Microsoft.Azure.Connectors.SharePoint.SPListExpandedUser",
          "Claims": "i:0#.f|membership|xxxx@test.com",
          "DisplayName": "Bunny",
          "Email": "xxxx@test.com",
          "Picture": "https://ts.xxxx.com/sites/applicationname/_layouts/15/UserPhoto.aspx?Size=L&AccountName=xxxx@test.com",
          "Department": 'null',
          "JobTitle": "Custom Software Engineering Sr Analyst"
        },
        "Editor#Claims": "i:0#.f|membership|xxxx@test.com",
        "{Identifier}": "Lists%252fDEX_XM_KPI_Stg_Mensual%252f3523_.000",
        "{IsFolder}": False,
        "{Thumbnail}": {
          "Large": 'null',
          "Medium": 'null',
          "Small": 'null'
        },
        "{Link}": "https://ts.xxxx.com/sites/applicationname/_layouts/15/listform.aspx?PageType=4&ListId=94a0db09%2Db3d5%2D4b7c%2D9e8d%2Da62b4483c805&ID=3523&ContentTypeID=0x01008F8A01EA6981B14198ECCD98F0B2A77C0035D4C03C1102FC4E81D1F8BA4B5F74E4",
        "{Name}": "111111",
        "{FilenameWithExtension}": "111111",
        "{Path}": "Lists/DEX_XM_KPI_Stg_Mensual/",
        "{FullPath}": "Lists/DEX_XM_KPI_Stg_Mensual/3523_.000",
        "{HasAttachments}": False,
        "{VersionNumber}": "5.0"
      }
    ]
  }
}

response_body_json = response_json.get('body')
response_value_json = response_body_json.get('value')
response_value_str = json.dumps(response_value_json)
print(response_value_str)
# item_schema = """
#     {
#         "namespace": "com.bunny.namespace",
#         "name": "real_value",
#         "type": "array",
#         "items": [
#             {"name": "@odata.etag", "type": "string"},
#             {"name": "ItemInternalId", "type": "string"},
#             {"name": "ID", "type": "int"},
#             {"name": "Title", "type": "string"},
#             {"name": "EID", "type": "string"},
#             {"name": "datekey", "type": "string"},
#             {"name": "ApplicationNm", "type": "string"},
#             {"name": "Portfolio", "type": "string"},
#             {"name": "AIRID", "type": "string"},
#             {"name": "dimension", "type": "string"},
#             {"name": "metric", "type": "string"},
#             {"name": "value", "type": "float"},
#             {"name": "Modified", "type": "string"},
#             {"name": "Created", "type": "string"},
#             {"name": "Author", "type": "record"},
#             {"name": "@odata.etag", "type": "int"},
#             {"name": "@odata.etag", "type": "int"},
#             {"name": "@odata.etag", "type": "int"},
#             {"name": "@odata.etag", "type": "int"},
#             {"name": "@odata.etag", "type": "int"},
#             {"name": "@odata.etag", "type": "int"},
#             {"name": "@odata.etag", "type": "int"},
#
#
#         ]
#     }
# """
#
#


real_value_schema={
  "name": "real_value",
  "type": "array",
  "namespace": "com.bunny.namespace",
  "items": {
    "name": "real_value_record",
    "type": "record",
    "fields": [
      {
        "name": "@odata.etag",
        "type": "string"
      },
      {
        "name": "ItemInternalId",
        "type": "string"
      },
      {
        "name": "ID",
        "type": "int"
      },
      {
        "name": "Title",
        "type": "string"
      },
      {
        "name": "EID",
        "type": "string"
      },
      {
        "name": "datekey",
        "type": "string"
      },
      {
        "name": "ApplicationNm",
        "type": "string"
      },
      {
        "name": "Portfolio",
        "type": "string"
      },
      {
        "name": "AIRID",
        "type": "string"
      },
      {
        "name": "dimension",
        "type": "string"
      },
      {
        "name": "metric",
        "type": "string"
      },
      {
        "name": "value",
        "type": "double"
      },
      {
        "name": "Modified",
        "type": "int",
        "logicalType": "date"
      },
      {
        "name": "Created",
        "type": "int",
        "logicalType": "date"
      },
      {
        "name": "Author",
        "type": {
          "name": "Author",
          "type": "record",
          "fields": [
            {
              "name": "@odata.type",
              "type": "string"
            },
            {
              "name": "Claims",
              "type": "string"
            },
            {
              "name": "DisplayName",
              "type": "string"
            },
            {
              "name": "Email",
              "type": "string"
            },
            {
              "name": "Picture",
              "type": "string"
            },
            {
              "name": "Department",
              "type": "string"
            },
            {
              "name": "JobTitle",
              "type": "string"
            }
          ]
        }
      },
      {
        "name": "Author#Claims",
        "type": "string"
      },
      {
        "name": "Editor",
        "type": {
          "name": "Editor",
          "type": "record",
          "fields": [
            {
              "name": "@odata.type",
              "type": "string"
            },
            {
              "name": "Claims",
              "type": "string"
            },
            {
              "name": "DisplayName",
              "type": "string"
            },
            {
              "name": "Email",
              "type": "string"
            },
            {
              "name": "Picture",
              "type": "string"
            },
            {
              "name": "Department",
              "type": "string"
            },
            {
              "name": "JobTitle",
              "type": "string"
            }
          ]
        }
      },
      {
        "name": "Editor#Claims",
        "type": "string"
      },
      {
        "name": "{Identifier}",
        "type": "string"
      },
      {
        "name": "{IsFolder}",
        "type": "boolean"
      },
      {
        "name": "{Thumbnail}",
        "type": {
          "name": "{Thumbnail}",
          "type": "record",
          "fields": [
            {
              "name": "Large",
              "type": "string"
            },
            {
              "name": "Medium",
              "type": "string"
            },
            {
              "name": "Small",
              "type": "string"
            }
          ]
        }
      },
      {
        "name": "{Link}",
        "type": "string"
      },
      {
        "name": "{Name}",
        "type": "string"
      },
      {
        "name": "{FilenameWithExtension}",
        "type": "string"
      },
      {
        "name": "{Path}",
        "type": "string"
      },
      {
        "name": "{FullPath}",
        "type": "string"
      },
      {
        "name": "{HasAttachments}",
        "type": "boolean"
      },
      {
        "name": "{VersionNumber}",
        "type": "string"
      }
    ]
  }
}
# print("12345678")