from dagster import job, op
from data_pipelines.pipelines.db_utils_pipeline import (
    DbUtilsPipeline,
)


@op
def op_truncate_pipeline_logs():
    db_utils_pipeline = DbUtilsPipeline()
    db_utils_pipeline.truncate_sql_table("metadata", "pipeline_logs")


@job
def job_truncate_pipeline_logs():
    op_truncate_pipeline_logs()
