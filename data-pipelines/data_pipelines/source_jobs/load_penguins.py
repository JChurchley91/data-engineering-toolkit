from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline
from dagster import op, job
from pandas import DataFrame

source_pipeline = SourceCsvPipeline("data_pipelines/source_jobs/penguins.yaml")


@op
def extract_penguins_data() -> DataFrame:
    df = source_pipeline.load_data_from_landing()
    return df


@op
def quality_check_penguins_data(df) -> bool:
    checks_passed = source_pipeline.perform_quality_checks(df)
    return checks_passed


@op
def write_penguins_to_target(df) -> None:
    source_pipeline.write_data_to_target(df)
    return None


@job
def load_penguins():
    df = extract_penguins_data()
    if quality_check_penguins_data(df):
        write_penguins_to_target(df)
