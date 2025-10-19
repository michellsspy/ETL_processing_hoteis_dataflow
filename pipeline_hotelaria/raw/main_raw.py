from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import apache_beam as beam
import logging
import sys
import os

from transforms.cl_get_tables_names import GetFolderNames
from transforms.cl_transient_to_raw import GcsCsvToBq

TAG_FAILED = 'failed'

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
    
    options = PipelineOptions(
        project='etl-hoteis',
        runner='DataflowRunner',
        streaming=False,
        job_name=f"etl-hoteis-{data_now}",
        temp_location='gs://bk-etl-hotelaria/temp_location',
        staging_location='gs://bk-etl-hotelaria/staging_location',
        autoscaling_algorithm='THROUGHPUT_BASED',
        worker_machine_type='n1-standard-4',
        num_workers=1,
        max_num_workers=3,
        disk_size_gb=25,
        region='us-central1',
        project_id='etl-hoteis',
        staging_bucket='bk-etl-hotelaria',
        save_main_session=True, 
        experiments=['use_runner_v2'],
        
        # Configurações do SDK Container
        prebuild_sdk_container_engine='cloud_build',
        docker_registry_push_url='us-central1-docker.pkg.dev/etl-hoteis/etl-hoteis-process/hoteis-dev',
        sdk_container_image='us-central1-docker.pkg.dev/etl-hoteis/etl-hoteis-process/hoteis-dev:latest',
        sdk_location='container',
        requirements_file='./requirements.txt',
        metabase_file='./metadata.json',
        setup_file='./setup.py',
        service_account_email='etl-hoteis@etl-hoteis.iam.gserviceaccount.com'
    )
    
    
    logger.info("Iniciando o pipeline com Runner V2 (lista) e DLQ (Corrigido)...")
    
    with beam.Pipeline(options=options) as p:
            folder_names = (
                p
                | "1. Iniciar Pipeline" >> beam.Create([None])
                | "2. Listar Pastas do GCS" >> beam.ParDo(GetFolderNames())
            )

            # Correção 2: Sintaxe do with_outputs
            results = (
                folder_names
                | "3. Carregar CSV do GCS para o BQ" >> beam.ParDo(GcsCsvToBq()).with_outputs(
                                                            TAG_FAILED
                                                        )
            )

            # Correção 3: Acessando sucesso com [None]
            (
                results[None]
                | "4.1. Exibir Sucessos" >> beam.Map(lambda r: logger.info(f"Carga bem-sucedida: {r}"))
            )

            (
                results[TAG_FAILED]
                | "4.2. Registrar Falhas (DLQ)" >> beam.Map(
                    lambda e: logger.error(f"FALHA NA CARGA (DLQ): {e}")
                    )
            )

if __name__ == '__main__':
    data_formatada = datetime.now().strftime('%Y%m%d%H%M%S')
    main_transient_to_raw(data_formatada)