# setup.py

from setuptools import setup, find_packages

setup(
    name="databricks_kafka_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark",
    ],
)
