from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from extract import cached_query_products
import pandas as pd


def elastic_indexation():
    client = Elasticsearch("http://localhost:9200")

    docs = []
    for i, row in cached_query_products().iterrows():
        doc = {
            '_index': 'pandas_test',
            '_id': i,
            '_source': {
                'id': row['id'],
            }
        }
        docs.append(doc)

    # Indexar documentos en Elasticsearch
    bulk(client, docs)

    client.close()


elastic_indexation()