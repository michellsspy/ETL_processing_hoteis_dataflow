# Importações necessárias (mantendo as suas originais que são úteis)
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
import apache_beam as beam
import pyarrow.parquet as pq  # Esta não é mais necessária para este DoFn
import pyarrow as pa         # Esta não é mais necessária para este DoFn
import pandas as pd
import logging
import sys
import io                     # Importante para ler o CSV da memória

# Monitoramento Logging (igual ao seu original)
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
    Este DoFn recebe um nome de pasta, lê o arquivo CSV correspondente
    no GCS, adiciona colunas de data (insert/update) e o carrega
    de forma incremental (append) em uma tabela do BigQuery.
    """

    def __init__(self):
        # Define os valores fixos no construtor
        self.projeto = "etl-hoteis"
        self.dataset_id = "raw_hotelaria"
        self.bucket_name = "bk-etl-hotelaria"
        
        # Clientes serão inicializados no 'setup'
        self.storage_client = None
        self.bq_client = None

    def setup(self):
        """
        Inicializa os clientes BQ e GCS no worker do Beam.
        """
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
            # Caminho do arquivo no GCS
            blob_path = f"transient/{folder_name}/{folder_name}.csv"
            
            # Nome da tabela no BQ
            table_name = f"raw_{folder_name}"
            
            # ID completo da tabela no BQ
            table_id = f"{self.projeto}.{self.dataset_id}.{table_name}"
            
            # Data e hora atuais para o insert_date
            now = datetime.now()

            logger.info(f"Iniciando processamento para: {folder_name}")
            logger.info(f"Lendo arquivo de: gs://{self.bucket_name}/{blob_path}")

            # 2. Leitura do arquivo CSV do GCS
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            
            if not blob.exists():
                logger.warning(f"Arquivo não encontrado: {blob_path}. Pulando este item.")
                yield f"Arquivo não encontrado: {blob_path}"
                return  # Para a execução deste elemento

            # Baixa o conteúdo do arquivo como bytes e decodifica para string
            content = blob.download_as_string().decode('utf-8')

            # 3. Transformação com Pandas
            # Usa io.StringIO para permitir que o pandas leia a string como se fosse um arquivo
            df = pd.read_csv(io.StringIO(content))

            # 4. Adiciona as novas colunas
            df['insert_date'] = pd.to_datetime(now)
            df['update_date'] = pd.NaT  # pd.NaT é convertido para NULL no BigQuery

            logger.info(f"Arquivo lido. {len(df)} linhas para carregar na tabela: {table_id}")

            # 5. Configuração da Carga no BigQuery
            job_config = bigquery.LoadJobConfig(
                # WRITE_APPEND = Carga Incremental. Anexa os dados na tabela.
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                
                # CREATE_IF_NEEDED = Cria a tabela se ela não existir.
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                
                # O esquema será inferido pelo Pandas DataFrame.
                # Se seus CSVs tiverem tipos complexos, talvez seja necessário
                # definir o esquema (schema) explicitamente aqui.
            )

            # 6. Executa a carga
            job = self.bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            
            job.result()  # Espera o job de carga ser concluído

            logger.info(f"Carga incremental concluída com sucesso para {table_id}.")
            
            # Retorna uma mensagem de sucesso para o próximo estágio
            yield f"Carga incremental para {table_id} concluída (Linhas: {len(df)})."

        except Exception as e:
            logger.error(f"Erro ao processar {folder_name}: {e}", exc_info=True)
            yield f"Erro ao processar {folder_name}: {e}"