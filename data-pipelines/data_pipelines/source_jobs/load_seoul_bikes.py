from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline
from dagster import op, job
from pandas import DataFrame

source_pipeline = SourceCsvPipeline("data_pipelines/source_jobs/seoul_bikes.yaml")


@op
def extract_seoul_bikes_data() -> DataFrame:
    df = source_pipeline.extract_data()
    return df


@op
def check_seoul_bikes_data(df) -> bool:
    checks_passed = source_pipeline.quality_checks(df)
    return checks_passed


@op
def insert_seoul_bikes_data(df) -> None:
    source_pipeline.insert_data(df)
    return None


@op
def write_seoul_bikes_logs(df):
    source_pipeline.write_logs(df)
    return None


@job
def load_seoul_bikes():
    df = extract_seoul_bikes_data()
    if check_seoul_bikes_data(df):
        insert_seoul_bikes_data(df)
        write_seoul_bikes_logs(df)
