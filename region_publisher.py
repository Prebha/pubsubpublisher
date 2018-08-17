#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import gzip
import logging
import argparse
import datetime
import json
from sodapy import Socrata
from google.cloud import pubsub

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TOPIC = 'Chicago_Traffic_Region_Live'
starttime=time.time()
dedup={}

if __name__ == '__main__':
    parser = \
        argparse.ArgumentParser(description='Send sensor data to Cloud Pub/Sub in small groups, simulating real-time behavior')
    args = parser.parse_args()
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.INFO)
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path('extended-argon-213022', TOPIC)
    publisher.get_topic(event_type)
    logging.info('Reusing pub/sub topic {}'.format(TOPIC))
    logging.info('Getting sensor data from Chicago Traffic Region API')
    client = Socrata('data.cityofchicago.org', None)
    while True:
        topublish = client.get('t2qc-9pjd')
        for x in topublish:
            if x['_region_id'] in dedup and x["_last_updt"]==dedup[x['_region_id']]:
                logging.info('Duplicate Entry Exists. Not publishing...')
            else:
                logging.info('Publishing to the queue...')
                publisher.publish(event_type, json.dumps(x).encode("utf-8"),origin='python',username='pubsub-publisher')
                dedup[x['_region_id']]=x["_last_updt"]
        logging.info("Will fetch new updates from the API after 10 mins...")
        time.sleep(600.0 - ((time.time() - starttime) % 600.0))
