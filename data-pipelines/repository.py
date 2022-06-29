from dagster import repository

from data_pipelines.source_jobs.load_penguins import job_load_penguins
from data_pipelines.source_jobs.load_seoul_bikes import job_load_seoul_bikes
from data_pipelines.source_jobs.load_hotel_bookings import job_load_hotel_bookings
from data_pipelines.target_jobs.cleanse_penguins import job_cleanse_penguins
from data_pipelines.target_jobs.cleanse_seoul_bikes import job_cleanse_seoul_bikes
from data_pipelines.target_jobs.cleanse_hotel_bookings import job_cleanse_hotel_bookings
from data_pipelines.util_jobs.truncate_logs import job_truncate_pipeline_logs


@repository
def source_load_jobs():
    jobs = [job_load_penguins, job_load_seoul_bikes, job_load_hotel_bookings]
    return jobs


@repository
def target_cleanse_jobs():
    jobs = [job_cleanse_penguins, job_cleanse_seoul_bikes, job_cleanse_hotel_bookings]
    return jobs


@repository
def util_jobs():
    jobs = [job_truncate_pipeline_logs]
    return jobs
