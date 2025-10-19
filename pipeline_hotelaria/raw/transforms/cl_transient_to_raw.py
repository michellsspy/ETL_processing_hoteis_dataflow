from apache_beam.pvalue import TaggedOutput
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
import apache_beam as beam
import pandas as pd
import logging
import sys
import io

# --- CORREÇÃO 1: Importar TaggedOutput ---
from apache_beam.pvalue import TaggedOutput

# Monitoramento Logging
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger(__name__)
logger = logging.getLogger(__name__)


class GcsCsvToBq(beam.DoFn):
    """
    Carrega CSV do GCS para o BQ.
    Emite para a saída principal (sucesso) ou para a tag 'failed' (DLQ).
    """

    # --- CORREÇÃO 2: Definir a tag de falha ---
    TAG_FAILED = 'failed'

    def __init__(self):
        self.projeto = "etl-hoteis"
        self.dataset_id = "raw_hotelaria"
        self.bucket_name = "bk-etl-hotelaria"
        self.storage_client = None
        self.bq_client = None

    def setup(self):
        try:
            self.storage_client = storage.Client()
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Clientes GCS e BigQuery inicializados com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao inicializar clientes: {e}")
            raise e

    def process(self, folder_name, *args, **kwargs):
        """
        Processa cada nome de pasta recebido do estágio anterior.
        """
        try:
            # 1. Monta os nomes e caminhos
            blob_path = f"transient/{folder_name}/{folder_name}.csv"
            partes = folder_name.split('_')  
            nome_sem_prefixo = '_'.join(partes[1:]) 
            table_name = f"raw_{nome_sem_prefixo}"
            table_id = f"{self.projeto}.{self.dataset_id}.{table_name}"
            now = datetime.now()

            logger.info(f"Iniciando processamento para: {folder_name}")
            logger.info(f"Lendo arquivo de: gs://{self.bucket_name}/{blob_path}")

            # 2. Leitura do arquivo CSV do GCS
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            
            # --- CORREÇÃO 3: Tratar falha (não encontrado) ---
            if not blob.exists():
                error_msg = f"Arquivo não encontrado: {blob_path}. Pulando este item."
                logger.warning(error_msg)
                # Emite para a tag de falha
                yield TaggedOutput(self.TAG_FAILED, error_msg)
                # REMOVIDO o 'return' que causava o Warning
            
            else:
                # 3. O resto do processo só ocorre se o arquivo existir
                content = blob.download_as_string().decode('utf-8')
                df = pd.read_csv(io.StringIO(content))

                # 4. Adiciona as novas colunas
                df['insert_date'] = pd.to_datetime(now)
                df['update_date'] = pd.NaT

                logger.info(f"Arquivo lido. {len(df)} linhas para carregar na tabela: {table_id}")

                # 5. Configuração da Carga no BigQuery
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                )

                # 6. Executa a carga
                job = self.bq_client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result()  # Espera o job de carga ser concluído

                success_msg = f"Carga incremental para {table_id} concluída (Linhas: {len(df)})."
                logger.info(success_msg)
                
                # --- CORREÇÃO 4: Emitir sucesso para a saída principal ---
                # Um 'yield' simples envia para a saída [None]
                yield success_msg

        except Exception as e:
            # --- CORREÇÃO 5: Tratar exceção geral (falha) ---
            error_msg = f"Erro ao processar {folder_name}: {e}"
            logger.error(error_msg, exc_info=True)
            # Emite para a tag de falha
            yield TaggedOutput(self.TAG_FAILED, error_msg)