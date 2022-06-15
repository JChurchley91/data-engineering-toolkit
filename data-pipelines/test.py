from dagster import op
from dagster import job
from data_pipelines.utils.config_loader import configLoader
from palmerpenguins import load_penguins
from boto3 import session
import pandas as pd

import os


@op
def get_penguins() -> pd.DataFrame:
    """
    Retrieve the penguins' dataset from palmerpenguins.
    Return the dataframe.
    """
    penguins = load_penguins()
    return penguins


@op
def write_penguins_csv() -> None:
    """
    Get penguins from get_penguins().
    Write the dataframe to a local csv file.
    """
    penguins = get_penguins()
    penguins.to_csv('penguins.csv', header=True, index=False)
    return None


@op
def check_penguin_csv() -> None:
    """
    TODO
    """
    penguins = pd.read_csv('penguins.csv')
    print(penguins.head())
    return None


@op
def load_penguins_to_storage() -> None:
    """
    Initiate a boto session and boto client.
    Upload penguins.csv to lemonheadwizards landing.
    Delete the csv file from local storage.
    """
    boto_session = session.Session()
    boto_client = boto_session.client('s3',
                                      region_name='fra1',
                                      endpoint_url='https://lemonheadwizards.fra1.digitaloceanspaces.com',
                                      aws_access_key_id='HZTCTYLALTQ6GLF4RYPD',
                                      aws_secret_access_key='5ev4tuA3Yp7OMks0WEjItSd6cfb/pzH4gob9W21ewUs')
    boto_client.upload_file('penguins.csv', 'landing', 'penguins.csv')
    os.remove('penguins.csv')
    return None


get_penguins()
write_penguins_csv()
check_penguin_csv()
load_penguins_to_storage()
