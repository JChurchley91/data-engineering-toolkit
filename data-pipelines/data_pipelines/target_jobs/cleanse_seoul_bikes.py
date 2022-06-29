from dagster import job, op
from data_pipelines.pipelines.target_sourced_to_cleansed_pipeline import (
    SourcedToCleansedPipeline,
)


@op
def op_cleanse_seoul_bikes():
    target_pipeline = SourcedToCleansedPipeline(
        "data_pipelines/target_jobs/configs/seoul_bikes.yaml"
    )
    target_pipeline.execute_pipeline()
    return None


@job
def job_cleanse_seoul_bikes():
    op_cleanse_seoul_bikes()
