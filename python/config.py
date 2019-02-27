import os, base64, re, logging
from elasticsearch import Elasticsearch



indexName='wargames'
type_name='sen_frig_sensor'
csvfile = open(r'MaritimeData/sen_frig_sensor.dsf', 'r')
num_of_shards=5
num_of_replica=1
# fieldnames of csv
fieldnames = ( "UKN","UKN","date-time","platform-name", "colour","ignore", "bearing","distance", "sensor-name","message")

# elasticsearch credentials
bonsai ='<url>'
auth = re.search('https\:\/\/(.*)\@', bonsai).group(1).split(':')
host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')
# Instantiate the new Elasticsearch connection:
es_header = [{
 'host': host,
 'port': 443,
 'use_ssl': True,
 'http_auth': (auth[0],auth[1])
}]
es = Elasticsearch(es_header)
print (es.ping)

