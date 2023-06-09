import ast
import pandas as pd
import psycopg2
import functools
import redis
import threading
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Creamos un cliente de Redis
redis_client = redis.Redis(host='localhost', port=6379)


# Decorador para cachear resultados de la función en Redis
def cache_function_results(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        # Generamos la clave del caché
        cache_key = f"{function.__name__}:{args}:{kwargs.items()}"

        # Intentamos obtener el resultado del caché
        cached_result = redis_client.get(cache_key)

        # Si existe el resultado en caché, lo devolvemos
        if cached_result is not None:
            return pd.read_json(cached_result)

        # Si no existe el resultado en caché, ejecutamos la función y lo almacenamos en caché
        result = function(*args, **kwargs)
        redis_client.set(cache_key, result.to_json())
        return result

    return wrapper


def query_products():
    # Connection postgres
    parameters = {
        "host": "127.0.0.1",
        "port": "5432",
        "user": "etl",
        "password": "123",
        "database": "customer"
    }

    conn = psycopg2.connect(**parameters)
    cur = conn.cursor()

    # Start the query
    cur.execute("""Select rp.id,rp.name,rp.code from res_partner rp""")

    # Get column names from cursor description
    column_names = [desc[0] for desc in cur.description]

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    dates = pd.DataFrame(resultados, columns=column_names)

    return dates


# Decorar la función query_products con el decorador de caché
@cache_function_results
def cached_query_products():
    return query_products()


def update_cache():
    while True:
        # Obtener la versión actual del caché
        cached_version = redis_client.get("query_version")

        # Obtener la versión actual de la consulta en la base de datos
        db_version = str(query_products().values)

        # Si la versión en la base de datos es diferente a la versión en caché,
        # actualizar el caché y la versión en caché
        if cached_version != db_version:
            redis_client.set("query_version", db_version)
            redis_client.delete(f"cached_query_products:{()}:dict_items([])")
            cached_query_products()
            print("Cache actualizado")

        # Esperar 10 minutos antes de volver a comprobar el caché
        time.sleep(10)


def redis_dates():

    # obtener los valores de las claves
    cached_query_odoo = redis_client.get("cached_query_products:():dict_items([])")

    # convertir los datos de bytes a una cadena de caracteres y luego a un diccionario
    cached_query_odoo = ast.literal_eval(cached_query_odoo.decode())

    df = pd.DataFrame(cached_query_odoo)
    return df


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


# Actualizar el caché al iniciar el programa
cached_query_products()

# Iniciar el hilo para actualizar el caché cada 10 minutos
threading.Thread(target=update_cache).start()

# Convertir los datos de Redis
redis_dates()

# Elastic search envio de Redis
while True:
    elastic_indexation()
    print("Agregando datos de Elastic")
    time.sleep(20)

