from dagster import job, op
from data_pipelines.pipelines.db_utils_pipeline import (
    DbUtilsPipeline,
)


@op
def op_truncate_pipeline_logs():
    db_utils_pipeline = DbUtilsPipeline("data_pipelines/util_jobs/configs/truncate_logs.yaml")
    db_utils_pipeline.execute_pipeline()
    return None


@job
def job_truncate_pipeline_logs():
    op_truncate_pipeline_logs()
