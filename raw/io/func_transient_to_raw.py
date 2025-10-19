from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import secretmanager
from datetime import datetime, timedelta
import apache_beam as beam
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

# Importando as funções
from transforms.cl_transient_to_raw import BQToParquet

def func_transient_to_raw(table_name, options, project_id, dataset_id):

    today = datetime.now()
    yesterday = today - timedelta(days=1)  # Calcula a data do dia anterior
    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    with beam.Pipeline(options=options) as pipeline:
        output_path_landing = f"gs://bk-etl-hotelaria/raw/{year}/{month}/{day}/{table_name}"  

        # Primeira parte do pipeline: Leitura do BigQuery e transformação para Raw
        landing_to_raw = (
            pipeline
            | f"Landing Read from BigQuery {table_name}" >> beam.io.ReadFromBigQuery(
                query=f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}_*`", 
                use_standard_sql=True,
                project=project_id
            )
            | f"Landing Transform Data {table_name}" >> beam.ParDo(BQToParquet())
            | f"Landing Write to GCS {table_name}" >> beam.io.WriteToText(
                                                                            output_path_landing,
                                                                            file_name_suffix=".json",
                                                                            shard_name_template='',
                                                                            num_shards=10  # Define o número de shards
                                                                        ) 
        )
        