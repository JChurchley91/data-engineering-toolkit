from datetime import datetime
from pandas import DataFrame
from data_pipelines.utils.yaml_utils import ConfigLoader
from data_pipelines.utils.sql_utils import SqlEngine
from data_pipelines.utils.sql_utils import DataReader
from data_pipelines.utils.sql_utils import DataWriter
from data_pipelines.utils.sql_utils import MetadataWriter
from data_pipelines.utils.sql_utils import LogWriter
from data_pipelines.utils.validator_utils import DFValidator


class BasePipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.config = ConfigLoader(self.job_name).get_config()
        self.pipeline_name = self.config["pipeline_name"]
        self.pipeline_id = self.config["pipeline_id"]
        self.sql_engine = SqlEngine().get_sql_engine()
        self.data_reader = DataReader()
        self.data_writer = DataWriter()
        self.metadata_writer = MetadataWriter()
        self.df_validator = DFValidator()
        self.log_writer = LogWriter(self.pipeline_id, self.pipeline_name)

    def __validate_df_exists(self, df):
        return self.df_validator.validate_df_exists(df)

    def add_metadata(self, df) -> DataFrame:
        df = self.metadata_writer.add_datetime_metadata(df)
        return df

    def insert_data_to_db(self, df, target_table_name, target_schema_name):
        self.data_writer.insert_data_to_database(
            df, target_table_name, target_schema_name)
        return None

    def write_logs(self, df, status, end_time):
        self.log_writer.write_logs(df, status, end_time)
        return None
