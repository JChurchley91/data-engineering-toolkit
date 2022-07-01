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
        self._job_name = job_name
        self._start_time = datetime.now()
        self._config = ConfigLoader(self._job_name).get_config()
        self._pipeline_name = self._config["pipeline_name"]
        self._pipeline_id = self._config["pipeline_id"]
        self._sql_engine = SqlEngine().get_sql_engine()
        self._data_reader = DataReader()
        self._data_writer = DataWriter()
        self._metadata_writer = MetadataWriter()
        self._df_validator = DFValidator()
        self._log_writer = LogWriter(self._pipeline_id, self._pipeline_name)

    def _validate_df_exists(self, df):
        return self._df_validator.validate_df_exists(df)

    def _add_metadata(self, df) -> DataFrame:
        df = self._metadata_writer.add_datetime_metadata(df)
        return df

    def _insert_data_to_db(self, df, target_table_name, target_schema_name):
        self._data_writer.insert_data_to_database(
            df, target_table_name, target_schema_name)
        return None

    def _write_logs(self, df, status, end_time):
        self._log_writer.write_logs(df, status, end_time)
        return None
