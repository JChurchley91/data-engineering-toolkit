from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline
from dagster import op, job
from pandas import DataFrame

source_pipeline = SourceCsvPipeline("data_pipelines/source_jobs/seoul_bikes.yaml")


@op
def extract_seoul_data() -> DataFrame:
    df = source_pipeline.load_data_from_landing()
    return df


@op
def quality_check_seoul_data(df) -> bool:
    checks_passed = source_pipeline.perform_quality_checks(df)
    return checks_passed


@op
def write_seoul_bikes_to_target(df) -> None:
    source_pipeline.write_data_to_target(df)
    return None


@job
def load_seoul_bikes():
    df = extract_seoul_data()
    if quality_check_seoul_data(df):
        write_seoul_bikes_to_target(df)
