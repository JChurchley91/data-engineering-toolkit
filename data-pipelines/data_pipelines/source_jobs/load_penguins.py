from data_pipelines.utils.yaml_loader import YamlLoader
from data_pipelines.pipelines.csv_pipeline import csvPipeline
from boto3 import session


def get_digital_ocean_config() -> list:
    yaml_loader = YamlLoader()
    config = yaml_loader.load_yaml_file('../config/digital_ocean.yaml')
    return config


def get_job_config() -> list:
    yaml_loader = YamlLoader()
    config = yaml_loader.load_yaml_file('penguins.yaml')
    return config


def write_penguins_to_target() -> None:
    """
    Initiate a boto session and boto client.
    Upload penguins.csv to lemonheadwizards landing.
    Delete the csv file from local storage.
    """
    digit_ocean_config = get_digital_ocean_config()
    boto_session = session.Session()
    boto_client = boto_session.client(
        "s3",
        region_name=f"{digit_ocean_config['region_name']}",
        endpoint_url=f"{digit_ocean_config['endpoint_url']}",
        aws_access_key_id=f"{digit_ocean_config['aws_access_key_id']}",
        aws_secret_access_key=f"{digit_ocean_config['aws_secret_access_key']}",
    )
    boto_client.upload_file("penguins.csv", "landing", "penguins.csv")
    return None


digital_ocean_config = get_digital_ocean_config()
job_config = get_job_config()
pipeline = csvPipeline(job_config, digital_ocean_config).execute_pipeline()
