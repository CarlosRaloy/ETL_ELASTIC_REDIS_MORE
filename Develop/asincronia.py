from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from redis_dates import redis_dates
import pandas as pd


def elastic_indexation():
    client = Elasticsearch("http://localhost:9200")

    docs = []
    for i, row in redis_dates().iterrows():
        doc = {
            '_index': 'pandas_test',
            '_id': i,
            '_source': {
                'id': row['id'],
                'name': row['name'],
                'code': row['code']
            }
        }
        docs.append(doc)

    # Indexar documentos en Elasticsearch
    bulk(client, docs)

    client.close()


elastic_indexation()