from datetime import datetime
from data_pipelines.utils.config_loader import ConfigLoader
from data_pipelines.utils.sql_engine import SqlEngine
from data_pipelines.utils.data_reader import DataReader
from data_pipelines.utils.data_writer import DataWriter
from data_pipelines.utils.log_writer import LogWriter


class SourcedToCleansedPipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.sql_engine = SqlEngine().get_sql_engine()
        self.config = ConfigLoader(self.job_name).get_config()
        self.target_job_id = self.config["target_job_id"]
        self.target_job_name = self.config["target_job_name"]
        self.sourced_table_name = self.config["sourced_table_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]
        self.data_reader = DataReader()
        self.data_writer = DataWriter()
        self.log_writer = LogWriter(self.target_job_id, self.target_job_name)
