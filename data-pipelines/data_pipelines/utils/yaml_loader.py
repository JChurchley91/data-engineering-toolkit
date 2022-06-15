import yaml
from yaml import safe_load


class yamlLoader:
    def __init__(self):
        pass

    @staticmethod
    def load_yaml_file(file_name):
        with open(f"{file_name}.yaml", "r") as stream:
            try:
                yaml_file_contents = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
            return yaml_file_contents
