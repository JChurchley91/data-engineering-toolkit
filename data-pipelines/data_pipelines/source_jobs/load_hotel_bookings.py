from data_pipelines.pipelines.source_csv_to_database_pipeline import (
    SourceCsvToDatabasePipeline,
)
from dagster import job, op


@op
def op_load_hotel_bookings():
    source_pipeline = SourceCsvToDatabasePipeline(
        "data_pipelines/source_jobs/configs/hotel_bookings.yaml"
    )
    source_pipeline.execute_pipeline()
    return None


@job
def job_load_hotel_bookings():
    op_load_hotel_bookings()
