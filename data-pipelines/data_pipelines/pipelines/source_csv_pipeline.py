import pandas as pd

from pandas import DataFrame
from data_pipelines.utils.yaml_loader import YamlLoader
from data_pipelines.utils.sql import get_sql_engine


class SourceCsvPipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.yaml_loader = YamlLoader()
        self.config = self.yaml_loader.load_yaml_file(self.job_name)
        self.engine = get_sql_engine()
        self.source_job_name = self.config["source_job_name"]
        self.source_job_id = self.config["source_job_id"]
        self.source_folder_name = self.config["source_folder_name"]
        self.source_file_name = self.config["source_file_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]

    def load_data_from_landing(self) -> DataFrame:
        df = pd.read_csv(
            f"../data/landing/{self.source_folder_name}/{self.source_file_name}"
        )
        return df

    def perform_quality_checks(self):
        pass

    def write_data_to_target(self, df):
        df.to_sql(
            name=self.target_table_name,
            con=self.engine,
            schema=self.target_schema_name,
            if_exists="replace",
            index=False,
        )
        return None

    def write_pipeline_logs(self):
        pass

    def execute_pipeline(self):
        try:
            df = self.load_data_from_landing()
        except FileNotFoundError:
            print(
                "file not found - please check the specified file name in yaml config!"
            )

        if len(df) > 0:
            self.write_data_to_target(df)
        else:
            print("dataframe is empty - no data will be inserted into database!")
