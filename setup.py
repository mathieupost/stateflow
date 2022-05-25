from setuptools import find_packages, setup

setup(
    name="stateflow",
    version="0.0.1",
    author="Wouter Zorgdrager",
    author_email="zorgdragerw@gmail.com",
    python_requires=">=3.6",
    packages=find_packages(exclude=("tests")),
    install_requires=[
        "graphviz",
        "libcst",
        "astpretty",
        "apache-beam",
        "ujson",
        "confluent-kafka",
        "beam-nuggets",
        "httplib2",
        "google-api-python-client",
        "pytest-mock",
        "pytest-docker-fixtures",
        "pytest-timeout",
        "apache-flink",
        "python-dynamodb-lock-whatnick",
        "pynamodb",
        "boto3",
        "fastapi",
        "uvicorn",
        "aiokafka",
        "httpx",
        "apache-flink-statefun",
        "aiohttp",
        "pyhamcrest",
        "graphene>=2.0",
        "tartiflette",
        "tartiflette-asgi==0.*",
        "starlette-graphene3",
        "jsonpickle",
    ],
)
