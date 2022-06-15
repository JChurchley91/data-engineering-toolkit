from pandas import DataFrame
import pandas as pd


class csvPipeline:
    def __init__(self, job_config, digital_ocean_config):
        self.job_config = job_config
        self.digital_ocean_config = digital_ocean_config

    @staticmethod
    def load_data_from_landing(job_config):
        df = pd.read_csv(f"../data/landing/{job_config['source_folder_name']}/{job_config['source_file_name']}")
        return df

    def write_data_locally(self, df):
        pass

    def perform_quality_checks(self, job_config):
        pass

    def write_data_to_target(self, df):
        pass

    def delete_local_data(self, job_config):
        pass

    def execute_pipeline(self):
        print(self.load_data_from_landing(self.job_config))

