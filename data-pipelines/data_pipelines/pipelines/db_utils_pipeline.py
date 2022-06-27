from data_pipelines.utils.sql import SqlEngine


class DbUtilsPipeline:
    def __init__(self):
        self.engine = SqlEngine.get_sql_engine()

    def truncate_sql_table(self, schema_name, table_name):
        statement = f"""TRUNCATE {schema_name}.{table_name} 
        RESTART IDENTITY CASCADE;"""
        self.engine.execute(statement)
        return None
