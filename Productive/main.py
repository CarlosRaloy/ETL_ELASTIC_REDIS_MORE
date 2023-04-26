from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from query import cached_query_odoo
import pandas as pd


def elastic_indexation():
    client = Elasticsearch("http://localhost:9200")

    docs = []
    for i, row in cached_query_odoo().iterrows():
        doc = {
            '_index': 'pandas_test',
            '_id': i,
            '_source': {
                'id_remision': row['id_remision'],
                'cliente': row['cliente'],
                'litros': row['litros'],
                'determinante': row['determinante'],
                'pedido_cliente': row['pedido_cliente'],
                'pedido_raloy': row['pedido_raloy'],
                'frr': pd.to_datetime(row['frr']).to_pydatetime() if not pd.isna(row['frr']) else None,
                'fp': pd.to_datetime(row['fp']).to_pydatetime() if not pd.isna(row['fp']) else None,
                'fpen0': pd.to_datetime(row['fpen0']).to_pydatetime() if not pd.isna(row['fpen0']) else None,
                'destino_final': row['destino_final'],
                'dias': row['dias'],
                'fecha_cierre': row['fecha_cierre'],
                'id_carta_porte': row['id_carta_porte'],
                'carta_porte': row['carta_porte']
            }
        }
        docs.append(doc)

    # Indexar documentos en Elasticsearch
    bulk(client, docs)

    client.close()


elastic_indexation()
