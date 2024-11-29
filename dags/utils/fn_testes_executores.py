import json
import psycopg2
import pandas as pd
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
import boto3
import hubspot
from dateutil.parser import isoparse
import pickle
from airflow.configuration import conf

from airflow.models import TaskInstance
from airflow.utils.state import State
import psutil


def print_executor_info(**context):
    ti = context["ti"]  # TaskInstance
    print(f"Executor: {ti.executor}")


def func_testes_executores(**context):

    print("MULTIPLOS EXECUTORES", conf.get("core", "executor"))
    # print_executor_info()

    executor = conf.get("core", "executor")
    print(f"The current Airflow executor is: {executor}")

    cpu_count = psutil.cpu_count(logical=False)  # Number of physical CPU cores
    cpu_logical_count = psutil.cpu_count(logical=True)  # Number of logical CPU cores
    memory_info = psutil.virtual_memory()  # Memory details

    resources = {
        "physical_cpu_cores": cpu_count,
        "logical_cpu_cores": cpu_logical_count,
        "total_memory": memory_info.total,  # Total memory in bytes
        "available_memory": memory_info.available,  # Available memory in bytes
    }

    print(f"Physical CPU Cores: {resources['physical_cpu_cores']}")
    print(f"Logical CPU Cores: {resources['logical_cpu_cores']}")
    print(f"Total Memory: {resources['total_memory'] / (1024 ** 3):.2f} GB")
    print(f"Available Memory: {resources['available_memory'] / (1024 ** 3):.2f} GB")

    ti = context["ti"]  # TaskInstance
    print(f"Executor: {ti.executor}")
