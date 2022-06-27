from data_pipelines.pipelines.source_csv_to_database_pipeline import SourceCsvToDatabasePipeline
from dagster import job


@job
def load_penguins():
    source_pipeline = SourceCsvToDatabasePipeline("data_pipelines/source_jobs/penguins.yaml")
    source_pipeline.execute_pipeline()
    return None
