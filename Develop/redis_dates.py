import redis
import json
import pandas as pd
import ast


def redis_dates():
    # conexión a la base de datos de Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    # obtener los valores de las claves
    cached_query_odoo = r.get("cached_query_dates_cool:():dict_items([])").decode('utf8')

    try:
        cached_query_odoo = ast.literal_eval(cached_query_odoo.decode())
        df = pd.DataFrame(cached_query_odoo)

    except Exception:
        data = json.loads(cached_query_odoo)
        df = pd.DataFrame.from_dict(data)

    print(df)


if __name__ == "__main__":
    redis_dates()