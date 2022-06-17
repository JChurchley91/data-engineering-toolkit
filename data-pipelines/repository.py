from dagster import repository

from data_pipelines.source_jobs.load_penguins import load_penguins


@repository
def source_load_jobs():
    jobs = [load_penguins]
    return jobs
