from pandas import DataFrame, read_csv


class DataReader:
    def __init__(self):
        pass

    @staticmethod
    def extract_csv_data(source_folder_name, source_file_name) -> DataFrame:
        df = read_csv(
            f"data_pipelines/data/landing/{source_folder_name}/{source_file_name}",
            encoding="unicode_escape",
        )
        return df
