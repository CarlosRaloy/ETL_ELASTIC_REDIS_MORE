from setuptools import setup

readme = open("./README.md", "r")

setup(
    name="redis_elastic",
    version="0.3.4",
    description="Crea una conexión de una base de datos postgres lo gurda en cache y en elasticsearch",
    install_requires=[
        "setuptools",
        "psycopg2",
        "psycopg2-binary",
        "redis",
        "elasticsearch==7.13.4",
        "pandas"
    ],
    long_description=readme.read(),
    long_description_content_type='text/markdown',
    author="Carlos Garcia Garcia",
    author_email="carlos.garcia1.gr1@icloud.com",
    # REPOSITORIO GIT
    url="https://github.com/CarlosRaloy/ETL_ELASTIC_REDIS_MORE",
    download_url='https://github.com/CarlosRaloy/ETL_ELASTIC_REDIS_MORE/tarball/01',
    kwargs=['redis', 'elasticsearch', 'warehouse'],
    license='MIT',
    packages=["Warehouse"],
    include_package_data=True
)
