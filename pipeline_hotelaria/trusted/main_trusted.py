from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import apache_beam as beam
import logging
import sys

# --- 1. Importando todas as suas classes da pasta 'transform' ---

from transforms.cl_transform_consumos import ProcessTrustedConsumos
from transforms.cl_transform_faturas import ProcessTrustedFaturas
from transforms.cl_transform_hospedes import ProcessTrustedHospedes
from transforms.cl_transform_hoteis import ProcessTrustedHoteis
from transforms.cl_transform_quartos import ProcessTrustedQuartos
from transforms.cl_transform_reservas import ProcessTrustedReservas
from transforms.cl_transform_reservas_ota import ProcessTrustedReservasOta

# --- 2. Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger(__name__)
logger = logging.getLogger(__name__)


def main_trusted(data_now):
    argv = sys.argv
    
    # --- 3. Opções do Pipeline (copiadas do seu main anterior) ---
    options = PipelineOptions(
        project='etl-hoteis',
        runner='DataflowRunner',
        streaming=False,
        # Novo nome para o Job da camada Trusted
        job_name=f"etl-hoteis-trusted-{data_now}", 
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
        save_main_session=False,
        prebuild_sdk_container_engine='cloud_build',
        docker_registry_push_url='us-central1-docker.pkg.dev/etl-hoteis/etl-hoteis-process/hoteis-dev',
        sdk_container_image='us-central1-docker.pkg.dev/etl-hoteis/etl-hoteis-process/hoteis-dev:latest',
        sdk_location='container',
        requirements_file='./requirements.txt',
        metabase_file='./metadata.json',
        setup_file='./setup.py',
        service_account_email='etl-743@etl-hoteis.iam.gserviceaccount.com'
    )
    
    # --- 4. Lógica do Pipeline (Execução Paralela) ---
    
    logger.info("Iniciando o pipeline da camada TRUSTED...")
    
    with beam.Pipeline() as p:
        
        # O 'start' é uma PCollection inicial que dispara o pipeline.
        # Ele contém apenas um elemento (None)
        start = (
            p 
            | "1. Iniciar Pipeline" >> beam.Create([None])
        )

        # A partir do 'start', criamos 7 ramos paralelos.
        # Cada ParDo(ProcessTrusted...()) recebe o 'None' e executa 
        # sua própria query MERGE no BigQuery.
        
        # Ramo 1: Consumos
        (start 
         | "2. Processar Trusted Consumos" >> beam.ParDo(ProcessTrustedConsumos())
         | "3. Log Consumos" >> beam.Map(lambda r: logger.info(f"Resultado Consumos: {r}"))
        )

        # Ramo 2: Faturas
        (start
         | "4. Processar Trusted Faturas" >> beam.ParDo(ProcessTrustedFaturas())
         | "5. Log Faturas" >> beam.Map(lambda r: logger.info(f"Resultado Faturas: {r}"))
        )

        # Ramo 3: Hospedes
        (start
         | "6. Processar Trusted Hospedes" >> beam.ParDo(ProcessTrustedHospedes())
         | "7. Log Hospedes" >> beam.Map(lambda r: logger.info(f"Resultado Hospedes: {r}"))
        )
        
        # Ramo 4: Hoteis
        (start
         | "8. Processar Trusted Hoteis" >> beam.ParDo(ProcessTrustedHoteis())
         | "9. Log Hoteis" >> beam.Map(lambda r: logger.info(f"Resultado Hoteis: {r}"))
        )

        # Ramo 5: Quartos
        (start
         | "10. Processar Trusted Quartos" >> beam.ParDo(ProcessTrustedQuartos())
         | "11. Log Quartos" >> beam.Map(lambda r: logger.info(f"Resultado Quartos: {r}"))
        )

        # Ramo 6: Reservas
        (start
         | "12. Processar Trusted Reservas" >> beam.ParDo(ProcessTrustedReservas())
         | "13. Log Reservas" >> beam.Map(lambda r: logger.info(f"Resultado Reservas: {r}"))
        )

        # Ramo 7: Reservas OTA
        (start
         | "14. Processar Trusted Reservas OTA" >> beam.ParDo(ProcessTrustedReservasOta())
         | "15. Log Reservas OTA" >> beam.Map(lambda r: logger.info(f"Resultado Reservas OTA: {r}"))
        )

    logger.info("Pipeline TRUSTED concluído.")

# --- 5. Bloco de Execução ---
if __name__ == '__main__':
    # Define a data atual para o nome do job
    data_formatada = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Chama a função principal que roda o pipeline
    main_trusted(data_formatada)