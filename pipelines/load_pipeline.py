import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import bigquery

# Define pipeline parameters.
# Dataflow Runner is used for its ability to perform multiple processes in parallel.
options = PipelineOptions(
    runner='DataFlowRunner',
    project='divine-aegis-386920',
    job_name='job1',
    temp_location='gs://dl-pf/temp',
    region='southamerica-west1')

# Define a function to read and process the original CSV file to match the expected data structure (JSONL or JSON Lines),
# which is required to correctly load the data into BigQuery data warehouse.
def csv_to_jsonl(row):
    import csv
    reader = csv.reader([row])
    columns = next(reader)
    country_name = columns[0]
    country_code = columns[1]
    indicator = columns[2]
    code = columns[3]
    numeric_columns = columns[4:]
    jsonl_data = {"country_name": country_name, "country_code": country_code, "indicator": indicator, "code": code}

    years = range(1960, 2024)
    for i, year in enumerate(years):
        jsonl_data[str(year)] = numeric_columns[i]

    return jsonl_data

# Set the location of the raw files and the respective table names.
raw_files = ['gs://dl-pf/Raw Data/inf_raw.csv', 'gs://dl-pf/Raw Data/mgn_raw.csv',
             'gs://dl-pf/Raw Data/pbi_raw.csv', 'gs://dl-pf/Raw Data/por_raw.csv',
             'gs://dl-pf/Raw Data/rem_raw.csv']

table_names = ['inflation', 'net_migration', 'gdp', 'percentage_migrants', 'remittances']

# Start the pipelines.
with beam.Pipeline(options=options) as pipeline:
    # Iterate over the different files to apply the same transformations.
    for i, file in enumerate(raw_files):
        table_name = table_names[i]

        # Skip the first 5 lines of the file because they contain unnecessary data.
        raw_data = (pipeline | f"Read {file}" >> beam.io.ReadFromText(file, skip_header_lines=5))

        # Apply the csv_to_jsonl function to the raw_data collection.
        jsonl_data = (raw_data | f'Convert_to_JsonL_{i}' >> beam.Map(csv_to_jsonl))

        table = f'divine-aegis-386920.dwh_automated.{table_name}'

        # Define the schema for the tables, specifying the names and types of the columns.
        schema = {"fields": [
            {"name": "country_name", "type": "STRING"}, {"name": "country_code", "type": "STRING"},
            {"name": "indicator", "type": "STRING"}, {"name": "code", "type": "STRING"},
            {"name": "1960", "type": "STRING"}, {"name": "1961", "type": "STRING"}, {"name": "1962", "type": "STRING"},
            {"name": "1963", "type": "STRING"}, {"name": "1964", "type": "STRING"}, {"name": "1965", "type": "STRING"},
            {"name": "1966", "type": "STRING"}, {"name": "1967", "type": "STRING"}, {"name": "1968", "type": "STRING"},
            {"name": "1969", "type": "STRING"}, {"name": "1970", "type": "STRING"}, {"name": "1971", "type": "STRING"},
            {"name": "1972", "type": "STRING"}, {"name": "1973", "type": "STRING"}, {"name": "1974", "type": "STRING"},
            {"name": "1975", "type": "STRING"}, {"name": "1976", "type": "STRING"}, {"name": "1977", "type": "STRING"},
            {"name": "1978", "type": "STRING"}, {"name": "1979", "type": "STRING"}, {"name": "1980", "type": "STRING"},
            {"name": "1981", "type": "STRING"}, {"name": "1982", "type": "STRING"}, {"name": "1983", "type": "STRING"},
            {"name": "1984", "type": "STRING"}, {"name": "1985", "type": "STRING"}, {"name": "1986", "type": "STRING"},
            {"name": "1987", "type": "STRING"}, {"name": "1988", "type": "STRING"}, {"name": "1989", "type": "STRING"},
            {"name": "1990", "type": "STRING"}, {"name": "1991", "type": "STRING"}, {"name": "1992", "type": "STRING"},
            {"name": "1993", "type": "STRING"}, {"name": "1994", "type": "STRING"}, {"name": "1995", "type": "STRING"},
            {"name": "1996", "type": "STRING"}, {"name": "1997", "type": "STRING"}, {"name": "1998", "type": "STRING"},
            {"name": "1999", "type": "STRING"}, {"name": "2000", "type": "STRING"}, {"name": "2001", "type": "STRING"},
            {"name": "2002", "type": "STRING"}, {"name": "2003", "type": "STRING"}, {"name": "2004", "type": "STRING"},
            {"name": "2005", "type": "STRING"}, {"name": "2006", "type": "STRING"}, {"name": "2007", "type": "STRING"},
            {"name": "2008", "type": "STRING"}, {"name": "2009", "type": "STRING"}, {"name": "2010", "type": "STRING"},
            {"name": "2011", "type": "STRING"}, {"name": "2012", "type": "STRING"}, {"name": "2013", "type": "STRING"},
            {"name": "2014", "type": "STRING"}, {"name": "2015", "type": "STRING"}, {"name": "2016", "type": "STRING"},
            {"name": "2017", "type": "STRING"}, {"name": "2018", "type": "STRING"}, {"name": "2019", "type": "STRING"},
            {"name": "2020", "type": "STRING"}, {"name": "2021", "type": "STRING"}, {"name": "2022", "type": "STRING"}, {"name": "2023", "type": "STRING"}]}

        # The data collection is loaded into BigQuery using the previously defined table names and schemas.
        # If the table doesn't exist, it is created using CREATE_IF_NEEDED, and if the table already exists,
        # it overwrites the previous data using WRITE_TRUNCATE.
        datos_jsonl | f'WriteToBigQuery_{i}' >> beam.io.gcp.bigquery.WriteToBigQuery(
            table=table,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

# The following call is used to run the pipelines. It should be executed in the Cloud Shell console.
""" gsutil cp gs://dl-pf/load_pipeline.py ~/load_pipeline.py
python ~/load_pipeline.py """