from datetime import datetime
from data_pipelines.utils.sql import SqlEngine


class LogWriter:
    def __init__(self, source_job_id, source_job_name, start_time):
        self.source_job_id = source_job_id
        self.source_job_name = source_job_name
        self.start_time = start_time
        self.engine = SqlEngine().get_sql_engine()

    def write_logs(self, df):
        data_length = len(df)
        end_time = datetime.now()
        statement = (
            f"insert into metadata.pipeline_logs"
            f"(pipeline_id, pipeline_name, start_time, end_time, rows_inserted, pipeline_status) "
            f"values "
            f"({self.source_job_id},"
            f"'{self.source_job_name}',"
            f"'{self.start_time}',"
            f"'{end_time}',"
            f"{data_length},"
            f"'succesful')"
        )
        self.engine.execute(statement)
        return None
