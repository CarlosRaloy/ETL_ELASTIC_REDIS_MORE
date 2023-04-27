import redis
import pandas as pd
import ast


def redis_dates():
    # conexión a la base de datos de Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    # obtener los valores de las claves
    cached_query_odoo = r.get("cached_query_products:():dict_items([])")

    # convertir los datos de bytes a una cadena de caracteres y luego a un diccionario
    cached_query_odoo = ast.literal_eval(cached_query_odoo.decode())

    df = pd.DataFrame(cached_query_odoo)
    return df


if __name__ == "__main__":
    redis_dates()
