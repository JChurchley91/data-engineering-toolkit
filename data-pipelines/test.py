from dagster import op
from dagster import job

from data_pipelines.utils.yaml_loader import configLoader
from data_pipelines.utils.data_loader import dataLoader
from data_pipelines.utils.data_checker import dataChecker
from data_pipelines.utils.data_writer import dataWriter
from data_pipelines.source_pipelines import csv_pipeline

from palmerpenguins import load_penguins
from boto3 import session
from pandas import DataFrame, read_csv

import os


@op
def get_penguins_df() -> DataFrame:
    """
    Retrieve the penguins' dataset from palmerpenguins.
    Return the dataframe.
    """
    penguins = load_penguins()
    return penguins


@op
def write_penguins_locally() -> None:
    """
    Get penguins from get_penguins().
    Write the dataframe to a local csv file.
    """
    penguins = get_penguins_df()
    penguins.to_csv("penguins.csv", header=True, index=False)
    return None


@op
def check_penguins_locally() -> None:
    """
    TODO
    """
    penguins = read_csv("penguins.csv")
    print(penguins.head())
    return None


@op
def write_penguins_to_storage() -> None:
    """
    Initiate a boto session and boto client.
    Upload penguins.csv to lemonheadwizards landing.
    Delete the csv file from local storage.
    """
    boto_session = session.Session()
    boto_client = boto_session.client(
        "s3",
        region_name="fra1",
        endpoint_url="https://lemonheadwizards.fra1.digitaloceanspaces.com",
        aws_access_key_id="HZTCTYLALTQ6GLF4RYPD",
        aws_secret_access_key="5ev4tuA3Yp7OMks0WEjItSd6cfb/pzH4gob9W21ewUs",
    )
    boto_client.upload_file("penguins.csv", "landing", "penguins.csv")
    os.remove("penguins.csv")
    return None


config = configLoader()
config_contents = config.read_yaml_config_file('data_pipelines/source_jobs/source_jobs_config')
print(config_contents)
