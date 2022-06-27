from data_pipelines.utils.yaml_loader import YamlLoader


class ConfigLoader:
    def __init__(self, job_name):
        self.job_name = job_name
        self.yaml_loader = YamlLoader()

    def get_config(self):
        config = self.yaml_loader.load_yaml_file(self.job_name)
        return config
