import config
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from sys import getsizeof
import csv
import json

try:
    config.es.indices.create(index=config.indexName,body={"settings" : {"index" : {"number_of_shards" : int(config.num_of_shards),"number_of_replicas" : int(config.num_of_replica)}}})
except:
    print ('Index exist. Overwriting')
    pass

reader = csv.DictReader( config.csvfile, config.fieldnames,delimiter='\t')
out = json.dumps( [ row for row in reader ] )
jsondata=json.loads(out)

def index_data():
 actions = []
 for i in jsondata:
  try:
   action = {
    "_index": config.indexName,
    "_type": config.type_name,
    "_source": i
   }
   actions.append(action)
  except Exception as e:
   print (e)

 try:
  for i in helpers.parallel_bulk(config.es, actions, chunk_size=len(actions), max_chunk_bytes=getsizeof(actions),
                                 raise_on_error=False):
   pass
 except Exception as e:
  print(e)

index_data()
