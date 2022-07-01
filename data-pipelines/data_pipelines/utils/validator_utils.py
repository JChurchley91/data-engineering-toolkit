class DFValidator:
    def __init__(self):
        pass

    @staticmethod
    def validate_df_exists(df):
        checks = []
        if len(df) > 0:
            checks.append(True)
        if all(checks):
            return True
        else:
            return False

    @staticmethod
    def remove_df_nulls(df, remove_nulls):
        df = df.drop("datetime_loaded", 1)
        df = df[df[remove_nulls].notnull()]
        return df
