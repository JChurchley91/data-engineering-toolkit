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
