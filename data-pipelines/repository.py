from dagster import repository

from data_pipelines.source_jobs.load_penguins import load_penguins
from data_pipelines.source_jobs.load_seoul_bikes import load_seoul_bikes


@repository
def source_load_jobs():
    jobs = [load_penguins, load_seoul_bikes]
    return jobs
