import os
from utility.drivers.onprem_postgre_driver import OnpremPostgresDriver
from utility.drivers.onprem_iotdb_driver import OnpremIoTDBDriver
from utility.drivers.onprem_rustfs_driver import OnpremRustfsDriver

from utility.drivers.azure_postgres_driver import AzurePostgresDriver
from utility.drivers.azure_sql_driver import AzureSQLDriver
from utility.drivers.azure_adls_driver import ADLSDriver

from utility.drivers.aws_postgres_driver import AwsPostgresDriver
from utility.drivers.aws_iotdb_driver import AwsIoTDBDriver
from utility.drivers.aws_s3_driver import S3Driver

from utility.drivers.onprem_redis_driver import OnpremRedisDriver
from utility.drivers.azure_redis_driver import AzureRedisDriver
from utility.drivers.aws_redis_driver import AwsRedisDriver


# PostgreDriver Connections 

def get_onprem_postgre_conn():
    return OnpremPostgresDriver().connect()

def get_azure_postgre_conn():
    return AzurePostgresDriver().connect()

def get_aws_postgre_conn():
    return AwsPostgresDriver().connect()


def get_postgre_connection(DEPLOYMENT_MODE):
    DEPLOYMENT_MODE = DEPLOYMENT_MODE.lower()

    if DEPLOYMENT_MODE == "on_prem":
        return get_onprem_postgre_conn()

    if DEPLOYMENT_MODE == "azure":
        return get_azure_postgre_conn()

    if DEPLOYMENT_MODE == "aws":
        return get_aws_postgre_conn()

    raise ValueError(f"Unsupported DEPLOYMENT_MODE: {DEPLOYMENT_MODE}")



# IoTDBDriver Connections 

def get_onprem_iotdb_conn():
    return OnpremIoTDBDriver().connect()

def get_azure_sql_conn():
    return AzureSQLDriver().connect()

def get_aws_iotdb_conn():
    return AwsIoTDBDriver().connect()


def get_iot_connection(DEPLOYMENT_MODE):
    DEPLOYMENT_MODE = DEPLOYMENT_MODE.lower()

    if DEPLOYMENT_MODE == "on_prem":
        return get_onprem_iotdb_conn()

    if DEPLOYMENT_MODE == "azure":
        return get_azure_sql_conn()

    if DEPLOYMENT_MODE == "aws":
        return get_aws_iotdb_conn()

    raise ValueError(f"Unsupported DEPLOYMENT_MODE: {DEPLOYMENT_MODE}")



# RustfsDriver Connections 

def get_onprem_rustfs_conn():
    return OnpremRustfsDriver().connect()

def get_azure_adls_conn():
    return ADLSDriver().connect()

def get_aws_s3_conn():
    return S3Driver().connect()


def get_rustfs_connection(DEPLOYMENT_MODE):
    DEPLOYMENT_MODE = DEPLOYMENT_MODE.lower()

    if DEPLOYMENT_MODE == "on_prem":
        return get_onprem_rustfs_conn()

    if DEPLOYMENT_MODE == "azure":
        return get_azure_adls_conn()

    if DEPLOYMENT_MODE == "aws":
        return get_aws_s3_conn()

    raise ValueError(f"Unsupported DEPLOYMENT_MODE: {DEPLOYMENT_MODE}")


# RedisDriver Connections
def redis_driver(DEPLOYMENT_MODE = os.getenv("DEPLOYMENT_MODE", "on_prem").lower()):
    DEPLOYMENT_MODE = DEPLOYMENT_MODE.lower()

    if DEPLOYMENT_MODE == "on_prem":
        return OnpremRedisDriver()

    if DEPLOYMENT_MODE == "azure":
        return AzureRedisDriver()

    if DEPLOYMENT_MODE == "aws":
        return AwsRedisDriver()

    raise ValueError(f"Unsupported DEPLOYMENT_MODE: {DEPLOYMENT_MODE}")