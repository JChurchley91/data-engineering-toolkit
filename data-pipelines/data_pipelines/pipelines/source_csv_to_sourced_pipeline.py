from pandas import DataFrame
from datetime import datetime
from data_pipelines.pipelines.base_pipeline import BasePipeline


class SourceCsvToDatabasePipeline(BasePipeline):
    def __init__(self, job_name):
        super().__init__(job_name)
        self.source_folder_name = self._config["source_folder_name"]
        self.source_file_name = self._config["source_file_name"]

    def extract_csv_data(self) -> DataFrame:
        df = self._data_reader.extract_csv_data(
            self.source_folder_name, self.source_file_name
        )
        return df

    def execute_pipeline(self):
        df = self.extract_csv_data()

        if self._validate_df_exists(df):
            self._add_metadata(df)
            self._insert_data_to_db(df, self._target_table_name, self._target_schema_name)
            self._write_logs(df, "successful", datetime.now())
        else:
            self._write_logs(df, "failed", datetime.now())
