from data_pipelines.pipelines.source_csv_pipeline import SourceCsvPipeline


source_pipeline = SourceCsvPipeline("penguins.yaml")
source_pipeline.execute_pipeline()
