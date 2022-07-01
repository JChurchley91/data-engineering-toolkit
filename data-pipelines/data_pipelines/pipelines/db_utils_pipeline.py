from data_pipelines.pipelines.base_pipeline import BasePipeline
from data_pipelines.utils.sql_utils import SqlEngine


class DbUtilsPipeline(BasePipeline):
    def __init__(self, job_name):
        super().__init__(job_name)
        self.engine = SqlEngine.get_sql_engine()

    def truncate_sql_table(self, schema_name, table_name):
        statement = f"""TRUNCATE {schema_name}.{table_name} 
        RESTART IDENTITY CASCADE;"""
        self.engine.execute(statement)
        return None

    def execute_pipeline(self):
        self.truncate_sql_table("metadata", "pipeline_logs")
