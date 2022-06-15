from data_pipelines.utils.yaml_loader import YamlLoader
from sqlalchemy import create_engine


def get_sql_config():
    loader = YamlLoader()
    sql_config = loader.load_yaml_file('../config/sql.yaml')
    return sql_config


def get_sql_engine():
    sql_config = get_sql_config()
    engine = create_engine(
        f"postgresql+psycopg2://{sql_config['username']}:{sql_config['password']}{sql_config['host']}"
        f":{sql_config['port']}/{sql_config['database']}"
    ).connect()
    return engine
