import datetime
import logging
from .src.KafkaProducer import KafkaProducer

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    producer = KafkaProducer()
    producer.run()
    

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
