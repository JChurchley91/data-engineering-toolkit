from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline
from dagster import op, job
from pandas import DataFrame

source_pipeline = SourceCsvPipeline("data_pipelines/source_jobs/penguins.yaml")


@op
def extract_penguins_data() -> DataFrame:
    df = source_pipeline.extract_data()
    return df


@op
def check_penguins_data(df) -> bool:
    checks_passed = source_pipeline.quality_checks(df)
    return checks_passed


@op
def insert_penguins_data(df) -> None:
    source_pipeline.insert_data(df)
    return None


@op
def write_penguins_logs(df):
    source_pipeline.write_logs(df)
    return None


@job
def load_penguins():
    df = extract_penguins_data()
    if check_penguins_data(df):
        insert_penguins_data(df)
        write_penguins_logs(df)
