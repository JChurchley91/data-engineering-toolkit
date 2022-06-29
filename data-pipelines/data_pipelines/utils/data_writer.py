from data_pipelines.utils.sql import SqlEngine


class DataWriter:
    def __init__(self):
        self.engine = SqlEngine().get_sql_engine()

    def insert_data_to_database(self, df, target_table_name, target_schema_name):
        df.to_sql(
            name=target_table_name,
            con=self.engine,
            schema=target_schema_name,
            if_exists="replace",
            index=False,
        )
        return None
