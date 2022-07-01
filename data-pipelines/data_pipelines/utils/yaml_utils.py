import yaml


class YamlLoader:
    def __init__(self):
        pass

    @staticmethod
    def load_yaml_file(file_name) -> list:
        with open(f"{file_name}", "r") as stream:
            try:
                yaml_file_contents = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
            return yaml_file_contents


class ConfigLoader:
    def __init__(self, job_name):
        self.job_name = job_name
        self.yaml_loader = YamlLoader()

    def get_config(self):
        config = self.yaml_loader.load_yaml_file(self.job_name)
        return config
