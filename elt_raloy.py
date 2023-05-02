import ast
import pandas as pd
import psycopg2
import functools
import redis
import threading
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def etl_raloy(query, redis_name, elasticsearch_name, time_redis, time_elastic, database, source):
    # Creamos un cliente de Redis
    redis_client = redis.Redis(host='localhost', port=6379)

    # Decorador para cachear resultados de la función en Redis
    def cache_function_results(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            # Generamos la clave del caché
            cache_key = f"{function.__name__}{redis_name}:{args}:{kwargs.items()}"

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

    def query_dates():
        # Connection postgres
        parameters = database

        conn = psycopg2.connect(**parameters)
        cur = conn.cursor()

        # Start the query
        cur.execute(query)

        # Get column names from cursor description
        column_names = [desc[0] for desc in cur.description]

        resultados = cur.fetchall()
        cur.close()
        conn.close()

        dates = pd.DataFrame(resultados, columns=column_names)

        return dates

    # Decorar la función query_products con el decorador de caché
    @cache_function_results
    def cached_query_dates():
        return query_dates()

    def update_cache():
        while True:
            # Obtener la versión actual del caché
            cached_version = redis_client.get(f"query_version{redis_name}")

            # Obtener la versión actual de la consulta en la base de datos
            db_version = str(query_dates().values)

            # Si la versión en la base de datos es diferente a la versión en caché,
            # actualizar el caché y la versión en caché
            if cached_version != db_version:
                redis_client.set(f"query_version{redis_name}", db_version)
                redis_client.delete(f"cached_query_dates{redis_name}:{()}:dict_items([])")
                cached_query_dates()
                print("Cache actualizado")

            # Redis, tiempo de actualization del cache
            time.sleep(time_redis)

    def redis_dates():

        # obtener los valores de las claves
        cached_query_redis = redis_client.get(f"cached_query_dates{redis_name}:():dict_items([])")

        try:
            cached_query_odoo = ast.literal_eval(cached_query_redis.decode())
            df = pd.DataFrame(cached_query_odoo)

        except Exception:
            data = json.loads(cached_query_redis)
            df = pd.DataFrame.from_dict(data)

        return df

    def elastic_indexation():
        client = Elasticsearch("http://localhost:9200")

        docs = []
        for i, row in redis_dates().iterrows():
            doc = {
                '_index': elasticsearch_name,
                '_id': i,
                '_source': {field: row[field] for field in source}
            }
            docs.append(doc)

        # Indexar documentos en Elasticsearch
        bulk(client, docs)
        client.close()

    # Actualizar el caché al iniciar el programa
    cached_query_dates()

    # Iniciar el hilo para actualizar el caché cada 10 minutos
    threading.Thread(target=update_cache).start()

    # Convertir los datos de Redis
    redis_dates()

    # Elastic search envio de Redis
    while True:
        elastic_indexation()
        print("Agregando datos de Elastic")
        time.sleep(time_elastic)


postgres = {
    "host": "127.0.0.1",
    "port": "5432",
    "user": "etl",
    "password": "123",
    "database": "customer"
}

list_fields = ['id', 'name', 'code']

query = """Select rp.id,rp.name,rp.code from res_partner rp"""

etl_raloy(query, "_productos", "productos", 10, 20, postgres, list_fields)
