from datetime import datetime
from data_pipelines.utils.config_loader import ConfigLoader
from data_pipelines.utils.sql_engine import SqlEngine
from data_pipelines.utils.data_reader import DataReader
from data_pipelines.utils.data_writer import DataWriter
from data_pipelines.utils.metadata_writer import MetadataWriter
from data_pipelines.utils.log_writer import LogWriter


class SourcedToCleansedPipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.sql_engine = SqlEngine().get_sql_engine()
        self.config = ConfigLoader(self.job_name).get_config()
        self.target_job_id = self.config["target_job_id"]
        self.target_job_name = self.config["target_job_name"]
        self.sourced_schema_name = self.config["sourced_schema_name"]
        self.sourced_table_name = self.config["sourced_table_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]
        self.data_reader = DataReader()
        self.data_writer = DataWriter()
        self.metadata_writer = MetadataWriter()
        self.log_writer = LogWriter(self.target_job_id, self.target_job_name)

    def extract_database_data(self):
        df = self.data_reader.extract_database_data(
            self.sourced_table_name, self.sourced_schema_name, self.sql_engine
        )
        return df

    @staticmethod
    def cleanse_database_data(df):
        df = df.drop("datetime_loaded", 1)
        return df

    def add_metadata(self, df):
        df = self.metadata_writer.add_datetime_metadata(df)
        return df

    def insert_data(self, df):
        self.data_writer.insert_data_to_database(
            df, self.target_table_name, self.target_schema_name
        )
        return None

    def write_logs(self, df, status, end_time):
        self.log_writer.write_logs(df, status, end_time)
        return None

    def execute_pipeline(self):
        df = self.extract_database_data()
        df = self.cleanse_database_data(df)
        df = self.add_metadata(df)

        self.insert_data(df)
        self.write_logs(df, "successful", datetime.now())
