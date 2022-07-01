class DFValidator:
    def __init__(self):
        pass

    @staticmethod
    def validate_df_exists(df):
        if len(df) > 0:
            return True
        else:
            return False

    @staticmethod
    def remove_df_nulls(df, remove_nulls):
        df = df[df[remove_nulls].notnull()]
        return df
