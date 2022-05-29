from dagster import repository

from data_pipelines.jobs.say_hello import say_hello_job
from data_pipelines.schedules.my_hourly_schedule import my_hourly_schedule
from data_pipelines.sensors.my_sensor import my_sensor


@repository
def data_pipelines():
    """
    The repository definition for this data_pipelines Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
