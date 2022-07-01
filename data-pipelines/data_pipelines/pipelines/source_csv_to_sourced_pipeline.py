from pandas import DataFrame
from datetime import datetime
from data_pipelines.pipelines.base_pipeline import BasePipeline


class SourceCsvToDatabasePipeline(BasePipeline):
    def __init__(self, job_name):
        super().__init__(job_name)
        self.source_folder_name = self.config["source_folder_name"]
        self.source_file_name = self.config["source_file_name"]
        self.target_table_name = self.config["target_table_name"]
        self.target_schema_name = self.config["target_schema_name"]

    def __extract_csv_data(self) -> DataFrame:
        df = self.data_reader.extract_csv_data(
            self.source_folder_name, self.source_file_name
        )
        return df

    def execute_pipeline(self):
        df = self.__extract_csv_data()

        if self.df_validator.validate_df_exists(df):
            self.add_metadata(df)
            self.insert_data_to_db(df, self.target_table_name, self.target_schema_name)
            self.write_logs(df, "successful", datetime.now())
        else:
            self.write_logs(df, "failed", datetime.now())
