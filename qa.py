from Develop.tools import action

postgres = {
    "host": "127.0.0.1",
    "port": "5432",
    "user": "etl",
    "password": "123",
    "database": "customer"
}

query = """Select rp.id,rp.name,rp.code from res_partner rp"""

# action.get_redis_dates("localhost", 6379, "cached_query_dates_test:():dict_items([])")
action.get_query_dates(postgres, query)
