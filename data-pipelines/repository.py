from dagster import repository

from data_pipelines.source_jobs.load_penguins import job_load_penguins
from data_pipelines.source_jobs.load_seoul_bikes import job_load_seoul_bikes
from data_pipelines.source_jobs.load_hotel_bookings import job_load_hotel_bookings


@repository
def source_load_jobs():
    jobs = [job_load_penguins, job_load_seoul_bikes, job_load_hotel_bookings]
    return jobs
