import sqlalchemy as sa


def get_sql_engine():
    engine = sa.create_engine(
        "postgresql+psycopg2://doadmin:AVNS_h_QaOUgRoYRpzXt@lemonheadwizards-do-user-11633707-0.b.db.ondigitalocean"
        ".com:25060/lemonheadwizards"
    ).connect()
    return engine
