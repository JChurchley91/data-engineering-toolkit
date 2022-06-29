from dagster import job, op
from data_pipelines.pipelines.target_sourced_to_cleansed_pipeline import (
    SourcedToCleansedPipeline,
)


@op
def op_cleanse_hotel_bookings():
    target_pipeline = SourcedToCleansedPipeline(
        "data_pipelines/target_jobs/configs/hotel_bookings.yaml"
    )
    target_pipeline.execute_pipeline()
    return None


@job
def job_cleanse_hotel_bookings():
    op_cleanse_hotel_bookings()
