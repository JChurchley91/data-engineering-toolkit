from pandas import DataFrame
from data_pipelines.utils.yaml_loader import YamlLoader
from boto3 import session

import pandas as pd
import os


class SourceCsvPipeline:
    def __init__(self, job_config):
        self.yaml_loader = YamlLoader()
        self.job_config = job_config
        self.source_job_name = self.job_config['source_job_name']
        self.source_job_id = self.job_config['source_job_id']
        self.source_folder_name = self.job_config['source_folder_name']
        self.source_file_name = self.job_config['source_file_name']
        self.target_folder_name = self.job_config['target_file_path_folder_name']
        self.target_file_name = self.job_config['target_file_path_file_name']

    def get_storage_config(self) -> list:
        config = self.yaml_loader.load_yaml_file('../config/storage.yaml')
        return config

    def load_data_from_landing(self) -> DataFrame:
        df = pd.read_csv(f"../data/landing/{self.source_folder_name}/{self.source_file_name}")
        return df

    def write_data_locally(self, df):
        df.to_csv(self.target_file_name, header=True, index=False)
        return None

    def perform_quality_checks(self):
        pass

    def write_data_to_target(self):
        storage_config = self.get_storage_config()
        boto_session = session.Session()
        boto_client = boto_session.client(
            "s3",
            region_name=f"{storage_config['region_name']}",
            endpoint_url=f"{storage_config['endpoint_url']}",
            aws_access_key_id=f"{storage_config['access_key_id']}",
            aws_secret_access_key=f"{storage_config['secret_access_key']}",
        )
        boto_client.upload_file(self.source_file_name, self.target_folder_name, self.target_file_name)
        return None

    def delete_local_data(self):
        if os.path.exists(self.source_file_name) and os.path.isfile(self.source_file_name):
            os.remove(self.source_file_name)

    def write_pipeline_logs(self):
        pass

    def execute_pipeline(self):
        data_found = False

        try:
            df = self.load_data_from_landing()
            data_found = True
        except FileNotFoundError:
            print('Data not found!')

        if data_found:
            self.write_data_locally(df)
            self.write_data_to_target()
            self.delete_local_data()
