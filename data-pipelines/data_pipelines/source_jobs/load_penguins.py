from dagster import job, op
from data_pipelines.pipelines.source_csv_to_database_pipeline import (
    SourceCsvToDatabasePipeline,
)


@op
def op_load_penguins():
    source_pipeline = SourceCsvToDatabasePipeline(
        "data_pipelines/source_jobs/configs/penguins.yaml"
    )
    source_pipeline.execute_pipeline()
    return None


@job
def job_load_penguins():
    op_load_penguins()
