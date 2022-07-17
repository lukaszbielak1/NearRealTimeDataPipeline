import sys
from confluent_kafka import Producer
import requests
import json
import os

class KafkaProducer:
    def __init__(self):
        self.CLOUDKARAFKA_BROKERS = os.getenv('CLOUDKARAFKA_BROKERS')
        self.BUS_API_URL = os.getenv('BUS_API_URL')
        self.CLOUDKARAFKA_USERNAME = os.getenv('CLOUDKARAFKA_USERNAME')
        self.CLOUDKARAFKA_PASSWORD = os.getenv('CLOUDKARAFKA_PASSWORD')
        self.CLOUDKARAFKA_TOPIC = os.getenv('CLOUDKARAFKA_TOPIC')
        self.API_COLUMNNAME = os.getenv('API_COLUMNNAME')

    def get_data_from_api(self):
        session = requests.Session()
        url = self.BUS_API_URL
        return session.get(url).text 

    def delivery_callback(self,err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))  
    def run(self):
        producer_config =  {
                                    'bootstrap.servers': self.CLOUDKARAFKA_BROKERS,
                                    'session.timeout.ms': 6000,
                                    'default.topic.config': {'auto.offset.reset': 'smallest'},
                                    'security.protocol': 'SASL_SSL',
	                                'sasl.mechanisms': 'SCRAM-SHA-256',
                                    'sasl.username': self.CLOUDKARAFKA_USERNAME,
                                    'sasl.password': self.CLOUDKARAFKA_PASSWORD
                                } 
        topic_name = self.CLOUDKARAFKA_TOPIC   
        producer = Producer(**producer_config)
        rows_from_api = json.loads(self.get_data_from_api())[self.API_COLUMNNAME]

        for row in rows_from_api:
            message = json.dumps(row)
            producer.produce(topic_name, message, callback=self.delivery_callback)
        producer.flush()
