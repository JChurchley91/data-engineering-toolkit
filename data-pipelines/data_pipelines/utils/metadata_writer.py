from datetime import datetime


class MetadataWriter:
    def __init__(self):
        self.pipeline_run_date = datetime.now()

    def add_datetime_metadata(self, df):
        df["datetime_loaded"] = self.pipeline_run_date
        return df
