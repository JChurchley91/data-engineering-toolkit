class DataChecker:
    def __init__(self):
        pass

    @staticmethod
    def check_df_exists(df):
        checks = []
        if len(df) > 0:
            checks.append(True)
        if all(checks):
            return True
        else:
            return False
