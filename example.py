from Warehouse.redis_elastic import Settings_redis_elastic

postgres = {
    "host": "127.0.0.1",
    "port": "5432",
    "user": "etl",
    "password": "123",
    "database": "customer"
}

query = """Select rp.id,rp.name,rp.code from res_partner rp"""

my_object = Settings_redis_elastic("localhost", "6379", "http://localhost:9200")
my_object.fusion(query, "_test", "test", 10, 20, postgres)

