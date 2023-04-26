import numpy as np
import pandas as pd
import psycopg2
import asyncio
import functools
import redis

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
        "host": "10.150.4.172",
        "port": "5433",
        "user": "lookerstudio",
        "password": "R3porte4d0r.",
        "database": "raloy_productivo"
    }

    conn = psycopg2.connect(**parameters)
    cur = conn.cursor()

    # Start the query
    cur.execute("""Select pt.default_code, pt.name from product_template pt""")

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


if __name__ == "__main__":
    dates = cached_query_products()
    print(dates)
