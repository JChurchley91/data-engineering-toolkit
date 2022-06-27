from data_pipelines.pipelines.source_csv_to_database_pipeline import SourceCsvToDatabasePipeline
from dagster import job


@job
def load_seoul_bikes():
    source_pipeline = SourceCsvToDatabasePipeline("data_pipelines/source_jobs/seoul_bikes.yaml")
    source_pipeline.execute_pipeline()
    return None
