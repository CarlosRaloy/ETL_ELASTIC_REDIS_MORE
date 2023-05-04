import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# ['id_remision', 'cliente', 'litros', 'determinante', 'pedido_cliente', 'pedido_raloy', 'frr', 'fp', 'fpen0', 'destino_final', 'dias', 'fecha_cierre', 'id_carta_porte', 'carta_porte']
def get_dates_pandas():
    list_pandas = [
        ('PANDA', 'Exotic', 7),
        ('Carole', 'Baskin', 7),
        ('Howard', 'Baskin', 6),
        ('PARDO', 'Finlay', 6),
        ('POLAR', 'Antle', 6),
    ]
    df = pd.DataFrame(list_pandas, columns=['First Name', 'Last Name', 'Total Episodes'])
    return df


def elastic_indexation():
    client = Elasticsearch("http://localhost:9200")
    docs = []
    for i, row in get_dates_pandas().iterrows():
        doc = {
            '_index': 'pandas_test',
            '_id': i,
            '_source': {
                'First Name': row['First Name'],
                'Last Name': row['Last Name'],
                'Total Episodes': row['Total Episodes']
            }
        }
        docs.append(doc)

    # Indexar documentos en Elasticsearch
    bulk(client, docs)

    client.close()


elastic_indexation()
