import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

# Defines a function that applies the same transformation to multiple tables.
def transform_tables(table):

    # Defines the pipeline parameters and chooses DirectRunner for its speed.
    options = PipelineOptions(
        runner='DirectRunner',
        project='divine-aegis-386920',
        job_name='trabajo1',
        temp_location='gs://dl-pf/temp',
        region='southamerica-west1')

    # Creates a pipeline to apply SQL statements (BigQuery) to multiple tables.
    with beam.Pipeline(options=options) as pipeline:
        
        sql_statements = f"""
        DELETE FROM `{table}`
        WHERE `nombrepais` NOT IN ('Argentina', 'Bolivia', 'Brasil', 'Canadá', 'Chile', 'Colombia', 'Costa Rica',
            'Cuba', 'Ecuador', 'El Salvador', 'Estados Unidos', 'Guatemala', 'Guyana', 'Haití', 'Honduras', 'Jamaica',
            'México', 'Nicaragua', 'Panamá', 'Paraguay', 'Perú', 'República Dominicana', 'Uruguay', 'Venezuela');
            """
        
        client = bigquery.Client()
        query_job = client.query(sql_statements)
        query_job.result()

        # Creates a dummy step that will be ignored since it's necessary for the pipeline.
        _ = pipeline | 'DummyStep' >> beam.Create([None])

# Tables previously created automatically with the pipelines_ETL.py file
tables = ['divine-aegis-386920.dwh_automatizado.inflacion', 
          'divine-aegis-386920.dwh_automatizado.migracion_neta', 
          'divine-aegis-386920.dwh_automatizado.pbi',
          'divine-aegis-386920.dwh_automatizado.porcentaje_migrantes',
          'divine-aegis-386920.dwh_automatizado.remesas']

# Calls the function for the 5 tables
for table in tables:
    transform_tables(table)

# The following call is used to run the pipelines. It should be executed in the Cloud Shell console.
""" gsutil cp gs://dl-pf/transform_pipeline.py ~/transform_pipeline.py
python ~/transform_pipeline.py """