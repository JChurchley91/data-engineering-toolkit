from data_pipelines.utils.yaml_loader import YamlLoader
from data_pipelines.pipelines.csv_pipeline import csvPipeline


def get_job_config() -> list:
    yaml_loader = YamlLoader()
    config = yaml_loader.load_yaml_file('penguins.yaml')
    return config


job_config = get_job_config()
pipeline = csvPipeline(job_config)
pipeline.execute_pipeline()
