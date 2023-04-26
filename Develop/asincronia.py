import numpy as np
import pandas as pd
import asyncpg
import asyncio
import functools


# Decorador para cachear resultados de la función
def cache_function_results(function):
    cache = {}

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        cache_key = (function.__name__, args, tuple(kwargs.items()))
        if cache_key in cache:
            return cache[cache_key]
        else:
            result = function(*args, **kwargs)
            cache[cache_key] = result
            return result

    return wrapper


async def query_products():
    # Connection postgres
    parameters = {
        "host": "10.150.4.172",
        "port": "5433",
        "user": "lookerstudio",
        "password": "R3porte4d0r.",
        "database": "raloy_productivo"
    }

    conn = await asyncpg.connect(**parameters)

    # Start the query
    resultados = await conn.fetch("""Select pt.default_code, pt.name from product_template pt""")

    await conn.close()

    column_names = resultados[0].keys()
    dates = pd.DataFrame(resultados, columns=column_names)

    return dates


# Decorar la función async_query_products con el decorador de caché
@cache_function_results
async def cached_query_products():
    return await query_products()


if __name__ == "__main__":
    dates = await cached_query_products()
    print(dates)
