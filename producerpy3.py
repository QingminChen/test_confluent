import json
import time
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth

from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# from confluent_kafka.avro import AvroProducer
from confluent_kafka import SerializingProducer
import uuid

from pyarrow.lib import null

KAFKA_TOPIC = "topic_1"


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


class RealValue(object):
    qingmin =[]

    def __init__(self, realvaluerecord):
        self.qingmin.append(realvaluerecord)
        print("1234")

    @staticmethod
    def realvalue_to_dict(realvalue, ctx):
        return RealValue.to_dict(realvalue)

    def to_dict(self):
        qingmin2 = []
        for i in self.qingmin:
            qingmin2.append(realvaluerecord_to_dict(i))
        return dict(qingmin=qingmin2)



class RealValueRecord(object):
    __slots__ = ["etag", "ID", "Modified"]

    #def __init__(self, etag,ID,Modified,Author):
    def __init__(self, etag, ID, Modified):
        self.etag=etag
        self.ID = ID
        self.Modified = Modified
        # self.Author = Author


def realvaluerecord_to_dict(realvaluerecord):
    return dict(etag=realvaluerecord.etag,ID=realvaluerecord.ID,Modified=realvaluerecord.Modified)


class Author(object):
    # __slots__ = ["type", "Claims", "DisplayName", "Email", "Picture", "Department", "JobTitle"]
    __slots__ = ["autype"]

    def __init__(self, autype):
        self.autype=autype

    @staticmethod
    def author_to_dict(author, ctx):
        return Author.to_dict(author)

    def to_dict(self):
        # return dict(author=dict(type=self.type,Claims=self.Claims,DisplayName=self.DisplayName,Email=self.Email,Picture=self.Picture,Department=self.Department,JobTitle=self.JobTitle))
        return dict(Author=dict(type=self.autype))


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
    "qingmin": [
      {
        "etag": "\"5\"",
        "ID": 3523,
        "Modified": "2022-11-21T08:50:29Z"
        #   ,
        # "Author": {
        #   "autype": "#Microsoft.Azure.Connectors.SharePoint.SPListExpandedUser"
        # }
      }
    ]
  }
}

response_body_json = response_json.get('body')
response_value_json = response_body_json.get('qingmin')
response_body_str = json.dumps(response_body_json)
response_value_str = json.dumps(response_value_json)
print(response_body_str)
print(response_value_str)

'''
   https://toolslick.com/generation/metadata/avro-schema-from-json
'''


real_value_schema_json={
  "name": "real_value",
  "type": "record",
  "fields": [
    {"name": "qingmin",
     "type":{
      "type": "array",
      "items": {
          "type": "record",
          "name": "real_value_record",
          "fields": [
            {
              "name": "etag",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "ID",
              "type": [
                "null",
                "int"
              ]
            },
            {
              "name": "Modified",
              "type": [
                "null",
                "string"
              ]
            }
          ]
      }
      }
    }
  ]
}



real_value_schema_str = json.dumps(real_value_schema_json)




schema_registry_conf = {
        'url': 'https://psrc-7qgn2.us-central1.gcp.confluent.cloud',
        'basic.auth.user.info': 'TSR5NTQACZWQMQKF:vlDVuIibhN6jJL/pwe8IwnyIsNna7kWv/joyzYswba0XXZqJOEc0370E5JKdXLoj'}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


name_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
                                          schema_str = name_schema,
                                          to_dict = Name.name_to_dict)
# count_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
#                                            schema_str =  count_schema,
#                                            to_dict = Count.count_to_dict)

real_value_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
                                           schema_str =  real_value_schema_str,
                                           to_dict = RealValue.realvalue_to_dict)



producer_conf = {}
producer_conf['bootstrap.servers']="pkc-3w22w.us-central1.gcp.confluent.cloud:9092"
producer_conf['security.protocol']='SASL_SSL'
producer_conf['sasl.mechanisms']='PLAIN'
# producer_conf['sasl.jaas.config']='org.apache.kafka.common.security.plain.PlainLoginModule'
# producer_conf['schema.registry.url']='https://psrc-30dr2.us-central1.gcp.confluent.cloud'
# producer_conf['basic.auth.user.info']='TBZ3QR4ZVVWC2HBM:Wr3lWLcPOmd7gZdTQBEro7FCBFODJW+qHikth1ESJ4Fsw/w14F2LlEcJ8T+qGYo6'
producer_conf['key.serializer']=name_avro_serializer
#producer_conf['value.serializer']=count_avro_serializer
# producer_conf['value.serializer']=real_value_avro_serializer
producer_conf['sasl.username']='XIAHOUQJEGPNA2BE'
producer_conf['sasl.password']='3CLrO2Voy1QHxNiIGx3em6wq3CC93PUCMIL1jVZjBAJgYpQTaDPwQTRTknr6UcZk'
# producer_conf['sasl.username']="testgcpusertwo@gmail.com"
# producer_conf['sasl.password']="2n*7db^f",
# producer_conf['enable.ssl.certificate.verification']=False
# conf.pop('basic.auth.credentials.source', None)

producer = SerializingProducer(producer_conf)
# producer2 = AvroProducer(
#     {'bootstrap.servers':'pkc-3w22w.us-central1.gcp.confluent.cloud:9092',
#
#     }
# )



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

import fastavro as fa
print(fa.validate(response_body_json,schema=real_value_schema_json,raise_errors=True,strict=True))



delivered_records = 0
for n in range(1):
    name_object = Name()
    name_object.name = "alice"
    # count_object = Count()
    # count_object.count = n
    print("Producing Avro record: {}\t{}".format(name_object.name, str(n)))
    # producer.produce(topic=KAFKA_TOPIC, key=name_object, value=response_value_json,on_delivery=acked)
    for i in response_body_json['qingmin']:

        '''["etag", "ItemInternalId", "ID", "Title", "EID", "datekey", "ApplicationNm","Portfolio","AIRID","dimension","metric","value","Modified","Created","AuthorClaims","EditorClaims","Identifier","IsFolder","Link","Name","FilenameWithExtension","Path","FullPath","HasAttachments","VersionNumber","Editor","Author"]'''
        #         RealValueRecord(etag=i['etag'], ItemInternalId=i['ItemInternalId'], ID=i['ID'], Title=i['Title'], EID=i['EID'],
        #                 datekey=i['datekey'], ApplicationNm=i['ApplicationNm'], Portfolio=i['Portfolio'],
        #                 AIRID=i['AIRID'], dimension=i['dimension'], metric=i['metric'], value=i['value'],
        #                 Modified=i['Modified'], Created=i['Created'], AuthorClaims=i['AuthorClaims'],
        #                 EditorClaims=i['EditorClaims'], Identifier=i['Identifier'], IsFolder=i['IsFolder'],
        #                 Link=i['Link'], Name=i['Name'], FilenameWithExtension=i['FilenameWithExtension'],
        #                 Path=i['Path'], FullPath=i['FullPath'], HasAttachments=i['HasAttachments'],
        #                 VersionNumber=i['VersionNumber'], Editor=i['Editor'], Author=i['Author'])
        real_value_record_list=[]

        # author=Author(autype=i['Author'].get('autype'))
        # real_value_record=RealValueRecord(etag=i['etag'],ID=i['ID'],Modified=i['Modified'],Author=author)
        real_value_record=RealValueRecord(etag=i['etag'],ID=i['ID'],Modified=i['Modified'])
        real_value_record_list.append(real_value_record)
        real_value=RealValue(real_value_record)
        # real_value['value']=real_value_record_list
        print("")
        #RealValueRecord(i.)

    producer.produce(topic=KAFKA_TOPIC, key=name_object, value=real_value_avro_serializer(RealValue,SerializationContext(KAFKA_TOPIC,MessageField.VALUE)), on_delivery=acked)
    producer.poll(0)
    time.sleep(0.025)

producer.flush()
