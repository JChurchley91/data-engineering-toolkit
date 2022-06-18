from dagster import repository

from data_pipelines.source_jobs.load_penguins import load_penguins
from data_pipelines.source_jobs.load_seoul_bikes import load_seoul_bikes
from data_pipelines.source_jobs.load_hotel_bookings import load_hotel_bookings


@repository
def source_load_jobs():
    jobs = [load_penguins, load_seoul_bikes, load_hotel_bookings]
    return jobs
