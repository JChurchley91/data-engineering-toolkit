from pandas import DataFrame, read_csv
from datetime import datetime
from data_pipelines.utils.config_loader import ConfigLoader
from data_pipelines.utils.sql import SqlEngine
from data_pipelines.utils.log_writer import LogWriter


class SourceCsvToDatabasePipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.engine = SqlEngine().get_sql_engine()
        self.config = ConfigLoader(self.job_name).get_config()
        self.source_job_name = self.config["source_job_name"]
        self.source_job_id = self.config["source_job_id"]
        self.source_folder_name = self.config["source_folder_name"]
        self.source_file_name = self.config["source_file_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]
        self.log_writer = LogWriter(self.source_job_id, self.source_job_name, self.start_time)

    def extract_data(self) -> DataFrame:
        df = read_csv(
            f"data_pipelines/data/landing/{self.source_folder_name}/{self.source_file_name}",
            encoding='unicode_escape'
        )
        return df

    def quality_checks(self, df):
        checks = []
        if len(df) > 0:
            checks.append(True)

        if all(checks):
            print(f"{self.job_name} has passed data checks!")
            return True
        else:
            return False

    def insert_data(self, df):
        df.to_sql(
            name=self.target_table_name,
            con=self.engine,
            schema=self.target_schema_name,
            if_exists="replace",
            index=False,
        )
        return None

    def write_logs(self, df):
        self.log_writer.write_logs(df)
        return None

    def execute_pipeline(self):
        df = self.extract_data()
        if self.quality_checks(df):
            self.insert_data(df)
            self.write_logs(df)
