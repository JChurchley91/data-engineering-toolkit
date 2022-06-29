from datetime import datetime, date
from data_pipelines.utils.sql_engine import SqlEngine


class LogWriter:
    def __init__(self, source_job_id, source_job_name, start_time):
        self.source_job_id = source_job_id
        self.source_job_name = source_job_name
        self.pipeline_run_date = date.today()
        self.start_time = start_time
        self.end_time = datetime.now()
        self.engine = SqlEngine().get_sql_engine()

    def write_logs(self, df, status, end_time):
        statement = (
            f"insert into metadata.pipeline_logs"
            f"(pipeline_id, pipeline_name, pipeline_run_date, start_time, end_time, data_row_count, pipeline_status) "
            f"values"
            f"({self.source_job_id},"
            f"'{self.source_job_name}',"
            f"'{self.pipeline_run_date}',"
            f"'{self.start_time}',"
            f"'{end_time}',"
            f"{len(df)},"
            f"'{status}')"
        )
        self.engine.execute(statement)
        return None
