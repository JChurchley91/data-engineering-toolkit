from datetime import datetime
from data_pipelines.pipelines.base_pipeline import BasePipeline


class SourcedToCleansedPipeline(BasePipeline):
    def __init__(self, job_name):
        super().__init__(job_name)
        self.sourced_schema_name = self._config["sourced_schema_name"]
        self.sourced_table_name = self._config["sourced_table_name"]
        self.target_table_name = self._config["target_table_name"]
        self.target_schema_name = self._config["target_schema_name"]
        self.remove_nulls = self._config["remove_nulls"]

    def __extract_database_data(self):
        df = self._data_reader.extract_database_data(
            self.sourced_table_name, self.sourced_schema_name)
        return df

    def __cleanse_database_data(self, df):
        self._df_validator.remove_df_nulls(df, self.remove_nulls)
        return df

    def execute_pipeline(self):
        df = self.__extract_database_data()

        if self._df_validator.validate_df_exists(df):
            df = self.__cleanse_database_data(df)
            df = self._add_metadata(df)
            self._insert_data_to_db(df, self.target_table_name, self.target_schema_name)
            self._write_logs(df, "successful", datetime.now())
        else:
            self._write_logs(df, "failed", datetime.now())
