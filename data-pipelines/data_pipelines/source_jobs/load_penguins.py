from data_pipelines.utils.yaml_loader import YamlLoader
from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline


def get_job_config() -> list:
    yaml_loader = YamlLoader()
    config = yaml_loader.load_yaml_file('penguins.yaml')
    return config


job_config = get_job_config()

source_pipeline = SourceCsvPipeline(job_config)
source_pipeline.execute_pipeline()
