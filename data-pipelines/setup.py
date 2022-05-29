import setuptools

setuptools.setup(
    name="data_pipelines",
    packages=setuptools.find_packages(exclude=["data_pipelines_tests"]),
    install_requires=[
        "dagster==0.14.17",
        "dagit==0.14.17",
        "pytest",
    ],
)
