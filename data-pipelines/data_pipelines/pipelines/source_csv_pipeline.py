from pandas import DataFrame, read_csv
from data_pipelines.utils.yaml_loader import YamlLoader
from data_pipelines.utils.sql import get_sql_engine


class SourceCsvPipeline:
    def __init__(self, job_name):
        self.job_name = job_name
        self.yaml_loader = YamlLoader()
        self.config = self.yaml_loader.load_yaml_file(self.job_name)
        self.engine = get_sql_engine()
        self.source_job_name = self.config["source_job_name"]
        self.source_job_id = self.config["source_job_id"]
        self.source_folder_name = self.config["source_folder_name"]
        self.source_file_name = self.config["source_file_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]

    def load_data_from_landing(self) -> DataFrame:
        df = read_csv(
            f"data_pipelines/data/landing/{self.source_folder_name}/{self.source_file_name}",
            encoding='unicode_escape'
        )
        return df

    def perform_quality_checks(self, df):
        checks = []
        if len(df) > 0:
            checks.append(True)

        if all(checks):
            print(f"{self.job_name} has passed data checks!")
            return True
        else:
            return False

    def write_data_to_target(self, df):
        df.to_sql(
            name=self.target_table_name,
            con=self.engine,
            schema=self.target_schema_name,
            if_exists="replace",
            index=False,
        )
        return None

    def write_pipeline_logs(self):
        statement = (
            f"insert into metadata.pipeline_logs"
            f"(pipeline_id, pipeline_name, start_time, end_time, pipeline_status) "
            f"values "
            f"({self.source_job_id},"
            f"'{self.job_name}',"
            f"'{self.start_time}',"
            f"'{self.end_time}',"
            f"'succesful')"
        )
        self.engine.execute(statement)
        return None
