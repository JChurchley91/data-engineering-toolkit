from pandas import DataFrame
from datetime import datetime
from data_pipelines.utils.config_loader import ConfigLoader
from data_pipelines.utils.sql import SqlEngine
from data_pipelines.utils.data_reader import DataReader
from data_pipelines.utils.data_writer import DataWriter
from data_pipelines.utils.df_validator import DFValidator
from data_pipelines.utils.log_writer import LogWriter
from data_pipelines.utils.metadata_writer import MetadataWriter
from sqlalchemy.exc import OperationalError


class SourceCsvToDatabasePipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.engine = SqlEngine().get_sql_engine()
        self.config = ConfigLoader(self.job_name).get_config()
        self.data_reader = DataReader()
        self.data_writer = DataWriter()
        self.data_checker = DFValidator()
        self.metadata_writer = MetadataWriter()
        self.source_job_name = self.config["source_job_name"]
        self.source_job_id = self.config["source_job_id"]
        self.source_folder_name = self.config["source_folder_name"]
        self.source_file_name = self.config["source_file_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]
        self.log_writer = LogWriter(
            self.source_job_id, self.source_job_name, self.start_time
        )

    def extract_data(self) -> DataFrame:
        df = self.data_reader.extract_csv_data(
            self.source_folder_name, self.source_file_name
        )
        return df

    def validate_df_exists(self, df):
        return self.data_checker.validate_df_exists(df)

    def add_metadata(self, df):
        df = self.metadata_writer.add_datetime_metadata(df)
        return df

    def insert_data(self, df):
        self.data_writer.insert_data_to_database(
            df, self.target_table_name, self.target_schema_name
        )
        return None

    def write_logs(self, df, status):
        self.log_writer.write_logs(df, status)
        return None

    def execute_pipeline(self):
        df = self.extract_data()

        if self.data_checker.check_df_exists(df):
            self.add_metadata(df)

            try:
                self.insert_data(df)
                self.write_logs(df, "successful")
            except OperationalError:
                self.write_logs(df, "failed")

        else:
            self.write_logs(df, "failed")
