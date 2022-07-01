import pandas as pd
from pandas import DataFrame, read_csv
from data_pipelines.utils.yaml_utils import YamlLoader
from sqlalchemy import create_engine
from datetime import datetime, date


class SqlEngine:
    def __init__(self):
        pass

    @staticmethod
    def get_sql_engine():
        loader = YamlLoader()
        sql_config = loader.load_yaml_file("data_pipelines/config/sql.yaml")
        engine = create_engine(
            f"postgresql+psycopg2://{sql_config['username']}:{sql_config['password']}{sql_config['host']}"
            f":{sql_config['port']}/{sql_config['database']}"
        ).connect()
        return engine


class DataReader:
    def __init__(self):
        self.sql_engine = SqlEngine().get_sql_engine()

    @staticmethod
    def extract_csv_data(source_folder_name, source_file_name) -> DataFrame:
        df = read_csv(
            f"data_pipelines/data/landing/{source_folder_name}/{source_file_name}",
            encoding="unicode_escape",
        )
        return df

    def extract_database_data(self, table_name, schema_name):
        df = pd.read_sql_table(table_name, schema=schema_name, con=self.sql_engine)
        return df


class DataWriter:
    def __init__(self):
        self.sql_engine = SqlEngine().get_sql_engine()

    def insert_data_to_database(self, df, target_table_name, target_schema_name):
        df.to_sql(
            name=target_table_name,
            con=self.sql_engine,
            schema=target_schema_name,
            if_exists="replace",
            index=False,
        )
        return None


class MetadataWriter:
    def __init__(self):
        self.pipeline_run_date = datetime.now()

    def add_datetime_metadata(self, df):
        df["datetime_loaded"] = self.pipeline_run_date
        return df


class LogWriter:
    def __init__(self, source_job_id, source_job_name):
        self.source_job_id = source_job_id
        self.source_job_name = source_job_name
        self.pipeline_run_date = date.today()
        self.start_time = datetime.now()
        self.engine = SqlEngine().get_sql_engine()

    def write_logs(self, df, status, end_time):
        statement = (
            f"insert into metadata.pipeline_logs"
            f"(pipeline_id, pipeline_name, pipeline_run_date, start_time, end_time, data_row_count, pipeline_status) "
            f"values"
            f"({self.source_job_id},"
            f"'{self.source_job_name}',"
            f"'{self.pipeline_run_date}',"
            f"'{self.start_time}',"
            f"'{end_time}',"
            f"{len(df)},"
            f"'{status}')"
        )
        self.engine.execute(statement)
        return None
