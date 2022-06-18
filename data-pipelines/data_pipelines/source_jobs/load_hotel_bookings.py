from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline
from dagster import op, job
from pandas import DataFrame

source_pipeline = SourceCsvPipeline("data_pipelines/source_jobs/hotel_bookings.yaml")


@op
def extract_hotel_bookings_data() -> DataFrame:
    df = source_pipeline.extract_data()
    return df


@op
def check_hotel_bookings_data(df) -> bool:
    checks_passed = source_pipeline.quality_checks(df)
    return checks_passed


@op
def insert_hotel_bookings_data(df) -> None:
    source_pipeline.insert_data(df)
    return None


@op
def write_hotel_bookings_logs(df):
    source_pipeline.write_logs(df)
    return None


@job
def load_hotel_bookings():
    df = extract_hotel_bookings_data()
    if check_hotel_bookings_data(df):
        insert_hotel_bookings_data(df)
        write_hotel_bookings_logs(df)
