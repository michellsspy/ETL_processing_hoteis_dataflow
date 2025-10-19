from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import secretmanager
from google.cloud import storage
from datetime import datetime, timedelta
import apache_beam as beam
import tempfile
import logging
import sys
import os

# --- MUDANÇA 1: Importando as classes corretas ---

# Importando a classe que lista pastas do GCS
from transforms.cl_get_tables_names import GetFolderNames

# Importando a classe que carrega CSV do GCS para o BQ
# (Assumindo que você salvou a nova classe 'GcsCsvToBq' 
# neste arquivo .py)
from transforms.cl_transient_to_raw import GcsCsvToBq


# Configuração do Logging (igual ao seu original)
# É uma boa prática ter o logging aqui no main
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger(__name__)
logger = logging.getLogger(__name__)


def main_transient_to_raw(data_now):
    argv = sys.argv
    
    # --- Configurações do Pipeline (iguais às suas) ---
    options = PipelineOptions(
        project='etl-hoteis',
        runner='DataflowRunner',
        streaming=False,
        job_name=f"etl-hoteis-{data_now}",
        temp_location='gs://bk-etl-hotelaria/temp_location',
        staging_location='gs://bk-etl-hotelaria/staging_location',
        #template_location=f'gs://bk-etl-hotelaria/templates/etl-siteprosite-{data_now}',
        autoscaling_algorithm='THROUGHPUT_BASED',
        worker_machine_type='n1-standard-4',
        num_workers=1,
        max_num_workers=3,
        disk_size_gb=25,
        region='us-central1',
        #zone='us-central1-c',
        #worker_zone='us-central1-a',
        project_id='etl-hoteis',
        staging_bucket='bk-etl-hotelaria',
        save_main_session=False,
        #experiments='use_runner_v2',
        prebuild_sdk_container_engine='cloud_build',
        docker_registry_push_url='us-central1-docker.pkg.dev/etl-hoteis/etl-hoteis-process/hoteis-dev',
        sdk_container_image='us-central1-docker.pkg.dev/etl-hoteis/etl-hoteis-process/hoteis-dev:latest',
        sdk_location='container',
        requirements_file='.\requirements.txt',
        metabase_file='.\metadata.json',
        setup_file='.\setup.py',
        service_account_email='etl-743@etl-hoteis.iam.gserviceaccount.com'
    )
    
    # --- MUDANÇA 2: Ajuste na lógica do Pipeline ---
    
    logger.info("Iniciando o pipeline...")
    
    with beam.Pipeline() as p:
            # Etapa 1: Listar as pastas no GCS
            folder_names = (
                p
                | "1. Iniciar Pipeline" >> beam.Create([None])
                | "2. Listar Pastas do GCS" >> beam.ParDo(GetFolderNames())
            )

            # Etapa 2: Ler cada CSV e salvar no BigQuery
            (
                folder_names
                | "3. Carregar CSV do GCS para o BQ" >> beam.ParDo(GcsCsvToBq())
                | "4. Exibir Resultados da Carga" >> beam.Map(logger.info)
            )

# --- MUDANÇA 3: Adicionando o bloco de execução ---
if __name__ == '__main__':
    # Define a data atual para o nome do job
    # (Você pode remover isso se 'data_now' vier de outro lugar)
    data_formatada = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Chama a função principal que roda o pipeline
    main_transient_to_raw(data_formatada)