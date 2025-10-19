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


class ProcessTrustedFaturas(beam.DoFn):
    """
    Este DoFn processa a tabela raw_faturas para a camada trusted.
    
    Ele executa uma query MERGE no BigQuery para:
    1. Buscar dados incrementais da camada raw.
    2. Corrigir os tipos de dados (STRING para DATE).
    3. Deduplicar os registros (mantendo o mais recente da raw).
    4. Inserir novos registros ou atualizar existentes na camada trusted.
    5. Gerenciar as colunas 'insert_date' e 'update_date' na trusted.
    """

    def __init__(self):
        self.projeto = "etl-hoteis"
        self.raw_dataset = "raw_hotelaria"
        self.trusted_dataset = "trusted_hotelaria"
        self.table_name = "faturas" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedFaturas.")
        except Exception as e:
            logger.error(f"Falha ao inicializar cliente BQ: {e}")
            raise e

    def get_last_processed_datetime(self, trusted_table_id):
        """
        Busca o 'insert_date' mais recente da tabela trusted para 
        processar apenas dados novos da raw.
        """
        try:
            query = f"SELECT MAX(insert_date) FROM `{trusted_table_id}`"
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            for row in results:
                max_date = row[0]
                if max_date:
                    logger.info(f"Último datetime encontrado em {trusted_table_id}: {max_date}")
                    return max_date.isoformat()
            
            logger.warning(f"Tabela {trusted_table_id} vazia. Usando data mínima.")
            return "1970-01-01T00:00:00"
            
        except Exception as e:
            logger.warning(f"Tabela {trusted_table_id} não encontrada. Iniciando carga total. Erro: {e}")
            return "1970-01-01T00:00:00"

    def create_table_if_not_exists(self, trusted_table_id):
        """
        Cria a tabela trusted com o schema correto se ela não existir.
        """
        schema = [
            # Colunas da raw (com tipos corrigidos)
            bigquery.SchemaField("fatura_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("reserva_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("hospede_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("data_emissao", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("data_vencimento", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("status_pagamento", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("forma_pagamento", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("subtotal_estadia", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("subtotal_consumos", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("descontos", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("impostos", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("valor_total", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("data_pagamento", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("taxa_limpeza", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("taxa_turismo", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("taxa_servico", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("numero_transacao", "STRING", mode="NULLABLE"),
            
            # Novas colunas de auditoria da Trusted
            bigquery.SchemaField("insert_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("update_date", "DATETIME", mode="NULLABLE"),
        ]
        
        table = bigquery.Table(trusted_table_id, schema=schema)
        
        try:
            self.bq_client.create_table(table, exists_ok=True)
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
        self.create_table_if_not_exists(trusted_table_id)

        # --- 3. Buscar Último datetime ---
        last_datetime = self.get_last_processed_datetime(trusted_table_id)

        # --- 4. Construção da Query SQL (MERGE) ---
        query = f"""
        MERGE `{trusted_table_id}` T
        USING (
            -- Subquery (Fonte 'S'): Seleciona, limpa e deduplica dados NOVOS da raw
            SELECT
                -- 1. Seleciona as colunas
                fatura_id,
                reserva_id,
                hospede_id,
                
                -- 2. Validação/Conversão de Tipos
                SAFE.PARSE_DATE('%Y-%m-%d', data_emissao) AS data_emissao,
                SAFE.PARSE_DATE('%Y-%m-%d', data_vencimento) AS data_vencimento,
                status_pagamento,
                forma_pagamento,
                subtotal_estadia,
                subtotal_consumos,
                descontos,
                impostos,
                valor_total,
                SAFE.PARSE_DATE('%Y-%m-%d', data_pagamento) AS data_pagamento,
                taxa_limpeza,
                taxa_turismo,
                taxa_servico,
                numero_transacao,

                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY fatura_id -- CHAVE da tabela
                        ORDER BY insert_date DESC
                    ) as rn
                FROM
                    `{raw_table_id}`
                WHERE
                    -- 5. FILTRO INCREMENTAL
                    insert_date > DATETIME('{last_datetime}')
            )
            WHERE rn = 1 -- Pega apenas o registro mais recente (deduplicação)
            
        ) S
        ON T.fatura_id = S.fatura_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'fatura_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                T.reserva_id = S.reserva_id,
                T.hospede_id = S.hospede_id,
                T.data_emissao = S.data_emissao,
                T.data_vencimento = S.data_vencimento,
                T.status_pagamento = S.status_pagamento,
                T.forma_pagamento = S.forma_pagamento,
                T.subtotal_estadia = S.subtotal_estadia,
                T.subtotal_consumos = S.subtotal_consumos,
                T.descontos = S.descontos,
                T.impostos = S.impostos,
                T.valor_total = S.valor_total,
                T.data_pagamento = S.data_pagamento,
                T.taxa_limpeza = S.taxa_limpeza,
                T.taxa_turismo = S.taxa_turismo,
                T.taxa_servico = S.taxa_servico,
                T.numero_transacao = S.numero_transacao,
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'fatura_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                fatura_id, reserva_id, hospede_id, data_emissao, data_vencimento,
                status_pagamento, forma_pagamento, subtotal_estadia, subtotal_consumos,
                descontos, impostos, valor_total, data_pagamento, taxa_limpeza,
                taxa_turismo, taxa_servico, numero_transacao,
                insert_date, update_date
            )
            VALUES (
                S.fatura_id, S.reserva_id, S.hospede_id, S.data_emissao, S.data_vencimento,
                S.status_pagamento, S.forma_pagamento, S.subtotal_estadia, S.subtotal_consumos,
                S.descontos, S.impostos, S.valor_total, S.data_pagamento, S.taxa_limpeza,
                S.taxa_turismo, S.taxa_servico, S.numero_transacao,
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
            
            stats = query_job.query_plan[0].step_info
            
            logger.info(f"MERGE concluído para {trusted_table_id}. Estatísticas: {stats}")
            yield f"Sucesso: {trusted_table_id} - {stats}"
            
        except Exception as e:
            logger.error(f"Erro ao executar MERGE para {self.table_name}: {e}", exc_info=True)
            yield f"Erro: {self.table_name} - {e}"