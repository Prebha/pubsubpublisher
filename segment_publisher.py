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
TOPIC = 'Chicago_Traffic_Segment_Live'
starttime=time.time()
dedup={}

if __name__ == '__main__':
    parser = \
        argparse.ArgumentParser(description='Send sensor data to Cloud Pub/Sub')
    args = parser.parse_args()
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.INFO)
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path('extended-argon-213022', TOPIC)
    publisher.get_topic(event_type)
    logging.info('Reusing pub/sub topic {}'.format(TOPIC))
    logging.info('Getting sensor data from Chicago Traffic Segment API')
    client = Socrata('data.cityofchicago.org', None)
    while True:
        topublish = client.get('8v9j-bter')
        for x in topublish:
            if x['segmentid'] in dedup and x["_last_updt"]==dedup[x['segmentid']]:
                logging.info('Duplicate Entry Exists. Not publishing...')
            else:
                logging.info('Publishing to the queue...')
                publisher.publish(event_type, json.dumps(x).encode("utf-8"),origin='python',username='pubsub-publisher')
                dedup[x['segmentid']]=x['_last_updt']
        logging.info("Will fetch new updates from the API after 10 mins...")
        time.sleep(600.0 - ((time.time() - starttime) % 600.0))
