from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timedelta

# Criando Def Options para reaproveitamento do código
def func_options(data_now):
    
    # Variáveis do projeto
    project_id = "etl-hoteis"
    argv = None

    return PipelineOptions(
        flags=argv,
        project=project_id,
        runner="DataflowRunner",
        streaming=False,
        job_name=f"etl-analytics-ga4-{data_now}",
        temp_location='gs://bk-etl-hotelaria/temp_location',
        staging_location='gs://bk-etl-hotelaria/staging_location',
        autoscaling_algorithm="THROUGHPUT_BASED",
        worker_machine_type="n1-standard-4",
        region="us-central1",  # Ou outra região, como "us-east1"
        zone="us-central1-c",  # Especifica uma zona diferente
        num_workers=1,  # Começar com mais workers pode ajudar na fase inicial
        max_num_workers=10,  # Permitir mais escalabilidade
        number_of_worker_harness_threads=2,  # Melhor aproveitamento das 16 vCPUs
        disk_size_gb=100,  # Evita gargalos por falta de espaço
        save_main_session=True,
        requirements_file="./requirements.txt",
        setup_file="./setup.py",
        service_account_email="etl-hoteis@etl-hoteis.iam.gserviceaccount.com"
    )