# Importações necessárias
from google.cloud import storage  # Mudamos de bigquery para storage
import apache_beam as beam
import logging
import sys

# Monitoramento Logging (igual ao seu original)
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger(__name__)
logger = logging.getLogger(__name__)

class GetFolderNames(beam.DoFn):
    """
    Este DoFn lista os nomes das "pastas" (prefixos) encontradas
    em um caminho específico do Google Cloud Storage (GCS).
    """
    def process(self, element, *args, **kwargs):
        # Define o bucket e o prefixo (caminho)
        bucket_name = "bk-etl-hotelaria"
        prefix_to_search = "transient/"

        # Função para listar as "pastas" (prefixos) no GCS
        def listar_pastas_gcs(bucket_name: str, prefix: str):
            """
            Lista prefixos (pastas) em um bucket do GCS.
            """
            try:
                client = storage.Client()
                
                # Usamos list_blobs com um delimitador '/'
                # Isso agrupa os resultados por "pasta"
                iterator = client.list_blobs(bucket_name, prefix=prefix, delimiter='/')
                
                # Precisamos iterar sobre o 'iterator' para que 
                # a propriedade .prefixes seja populada.
                list(iterator) 
                
                pastas_encontradas = []
                
                # Os nomes das "pastas" são retornados em .prefixes
                if iterator.prefixes:
                    for folder_prefix in iterator.prefixes:
                        # O retorno será algo como: "transient/nome_da_pasta/"
                        
                        # 1. Remove o prefixo inicial ("transient/"):
                        nome_pasta = folder_prefix[len(prefix):] 
                        
                        # 2. Remove a barra final ("/"):
                        nome_pasta_limpo = nome_pasta.strip('/')
                        
                        # Adiciona à lista apenas se o nome não estiver vazio
                        if nome_pasta_limpo:
                            pastas_encontradas.append(nome_pasta_limpo)
                            
                # Usamos 'set' para garantir nomes únicos, caso haja repetição
                return list(set(pastas_encontradas))

            except Exception as e:
                logger.error(f"Erro ao conectar ou listar pastas no GCS: {e}")
                raise e

        # --- Execução Principal do DoFn ---
        
        logger.info(f"Iniciando busca por pastas em gs://{bucket_name}/{prefix_to_search}")
        
        # Chama a função para listar as pastas
        nomes_das_pastas = listar_pastas_gcs(bucket_name, prefix_to_search)
        
        logger.info(f"Pastas encontradas: {nomes_das_pastas}")
        
        # Gera os nomes das pastas para o próximo estágio do pipeline
        for nome_pasta in nomes_das_pastas:
            yield nome_pasta  # Retorna os nomes das pastas, um por um