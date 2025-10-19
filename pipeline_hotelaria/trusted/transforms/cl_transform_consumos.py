import apache_beam as beam
import logging
import sys
from datetime import datetime
from google.cloud import bigquery

# Monitoramento Logging (igual ao seu original)
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger(__name__)
logger = logging.getLogger(__name__)


class ProcessTrustedConsumos(beam.DoFn):
    """
    Este DoFn processa a tabela raw_consumos para a camada trusted.
    
    Ele executa uma query MERGE no BigQuery para:
    1. Buscar dados incrementais da camada raw.
    2. Corrigir os tipos de dados (STRING para DATE/TIME).
    3. Deduplicar os registros (mantendo o mais recente da raw).
    4. Inserir novos registros ou atualizar existentes na camada trusted.
    5. Gerenciar as colunas 'insert_date' e 'update_date' na trusted.
    """

    def __init__(self):
        self.projeto = "etl-hoteis"
        self.raw_dataset = "raw_hotelaria"
        self.trusted_dataset = "trusted_hotelaria"
        self.table_name = "consumos" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedConsumos.")
        except Exception as e:
            logger.error(f"Falha ao inicializar cliente BQ: {e}")
            raise e

    def get_last_processed_datetime(self, trusted_table_id):
        """
        Busca o 'insert_date' mais recente da tabela trusted para 
        processar apenas dados novos da raw.
        """
        try:
            # Esta query busca o último 'insert_date' carregado na trusted.
            query = f"SELECT MAX(insert_date) FROM `{trusted_table_id}`"
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            for row in results:
                max_date = row[0]
                if max_date:
                    logger.info(f"Último datetime encontrado em {trusted_table_id}: {max_date}")
                    return max_date.isoformat()
            
            # Se a tabela estiver vazia, usamos uma data "mínima"
            logger.warning(f"Tabela {trusted_table_id} vazia. Usando data mínima.")
            return "1970-01-01T00:00:00"
            
        except Exception as e:
            # Provavelmente a tabela não existe ainda (primeira execução)
            logger.warning(f"Tabela {trusted_table_id} não encontrada. Iniciando carga total. Erro: {e}")
            return "1970-01-01T00:00:00"

    def create_table_if_not_exists(self, trusted_table_id):
        """
        Cria a tabela trusted com o schema correto se ela não existir.
        O MERGE precisa que a tabela exista antes de ser executado.
        """
        schema = [
            # Colunas da raw (com tipos corrigidos)
            bigquery.SchemaField("consumo_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("reserva_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("hospede_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("hotel_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("nome_servico", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data_consumo", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("quantidade", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("valor_total_consumo", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("hora_consumo", "TIME", mode="NULLABLE"), # STRING -> TIME
            bigquery.SchemaField("local_consumo", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("funcionario_responsavel", "STRING", mode="NULLABLE"),
            
            # Novas colunas de auditoria da Trusted
            bigquery.SchemaField("insert_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("update_date", "DATETIME", mode="NULLABLE"),
        ]
        
        table = bigquery.Table(trusted_table_id, schema=schema)
        
        try:
            self.bq_client.create_table(table, exists_ok=True) # exists_ok=True não faz nada se ela já existir
            logger.info(f"Tabela {trusted_table_id} verificada/criada com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao criar tabela {trusted_table_id}: {e}")
            raise

    def process(self, element, *args, **kwargs):
        """
        Executa a lógica de transformação MERGE no BigQuery.
        """
        
        # --- 1. Definição de Nomes ---
        raw_table_id = f"{self.projeto}.{self.raw_dataset}.raw_{self.table_name}"
        trusted_table_id = f"{self.projeto}.{self.trusted_dataset}.trusted_{self.table_name}"
        
        logger.info(f"Iniciando processamento incremental da {raw_table_id} para {trusted_table_id}...")

        # --- 2. Garantir que a Tabela Trusted Exista ---
        # O MERGE falhará se a tabela de destino não existir.
        self.create_table_if_not_exists(trusted_table_id)

        # --- 3. Buscar Último datetime ---
        # Para processar apenas dados novos (carga incremental)
        last_datetime = self.get_last_processed_datetime(trusted_table_id)

        # --- 4. Construção da Query SQL (MERGE) ---
        query = f"""
        MERGE `{trusted_table_id}` T
        USING (
            -- Subquery (Fonte 'S'): Seleciona, limpa e deduplica dados NOVOS da raw
            SELECT
                -- 1. Seleciona as colunas (exceto insert/update da raw)
                consumo_id,
                reserva_id,
                hospede_id,
                hotel_id,
                nome_servico,
                
                -- 2. Validação/Conversão de Tipos (Schema Validation)
                -- Usamos SAFE.PARSE_DATE para não falhar se a data for inválida (vira NULL)
                SAFE.PARSE_DATE('%Y-%m-%d', data_consumo) AS data_consumo,
                quantidade,
                valor_total_consumo,
                SAFE.PARSE_TIME('%H:%M:%S', hora_consumo) AS hora_consumo,
                local_consumo,
                funcionario_responsavel,
                
                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    -- Rankeia os registros por 'consumo_id', ordenando pelo 'insert_date' da raw.
                    ROW_NUMBER() OVER(
                        PARTITION BY consumo_id 
                        ORDER BY insert_date DESC
                    ) as rn
                FROM
                    `{raw_table_id}`
                WHERE
                    -- 5. FILTRO INCREMENTAL: Processa apenas dados mais novos que a última carga
                    insert_date > DATETIME('{last_datetime}')
            )
            WHERE rn = 1 -- Pega apenas o registro mais recente (deduplicação)
            
        ) S
        ON T.consumo_id = S.consumo_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'consumo_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                -- Atualiza todos os campos (exceto a chave e o insert_date original)
                T.reserva_id = S.reserva_id,
                T.hospede_id = S.hospede_id,
                T.hotel_id = S.hotel_id,
                T.nome_servico = S.nome_servico,
                T.data_consumo = S.data_consumo,
                T.quantidade = S.quantidade,
                T.valor_total_consumo = S.valor_total_consumo,
                T.hora_consumo = S.hora_consumo,
                T.local_consumo = S.local_consumo,
                T.funcionario_responsavel = S.funcionario_responsavel,
                -- Mantém o insert_date original da linha
                -- Atualiza o update_date como pedido
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'consumo_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                consumo_id, reserva_id, hospede_id, hotel_id, nome_servico,
                data_consumo, quantidade, valor_total_consumo, hora_consumo,
                local_consumo, funcionario_responsavel,
                insert_date, update_date
            )
            VALUES (
                S.consumo_id, S.reserva_id, S.hospede_id, S.hotel_id, S.nome_servico,
                S.data_consumo, S.quantidade, S.valor_total_consumo, S.hora_consumo,
                S.local_consumo, S.funcionario_responsavel,
                S.insert_date, -- A data/hora atual
                S.update_date  -- O valor NULL
            )
        ;
        """

        # --- 5. Execução da Query ---
        try:
            logger.info(f"Executando query MERGE para {self.table_name}...")
            
            query_job = self.bq_client.query(query)
            query_job.result()  # Espera a query terminar
            
            # Captura estatísticas do job
            stats = query_job.query_plan[0].step_info
            
            logger.info(f"MERGE concluído para {trusted_table_id}. Estatísticas: {stats}")
            yield f"Sucesso: {trusted_table_id} - {stats}"
            
        except Exception as e:
            logger.error(f"Erro ao executar MERGE para {self.table_name}: {e}", exc_info=True)
            yield f"Erro: {self.table_name} - {e}"
            