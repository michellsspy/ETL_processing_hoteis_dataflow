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


class ProcessTrustedReservasOta(beam.DoFn):
    """
    Este DoFn processa a tabela raw_reservas_ota para a camada trusted.
    
    Ele executa uma query MERGE no BigQuery para:
    1. Buscar dados incrementais da camada raw.
    2. Deduplicar os registros (mantendo o mais recente da raw).
    3. Inserir novos registros ou atualizar existentes na camada trusted.
    4. Gerenciar as colunas 'insert_date' e 'update_date' na trusted.
    """

    def __init__(self):
        self.projeto = "etl-hoteis"
        self.raw_dataset = "raw_hotelaria"
        self.trusted_dataset = "trusted_hotelaria"
        self.table_name = "reservas_ota" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedReservasOta.")
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
            # Colunas da raw (tipos já corretos)
            bigquery.SchemaField("ota_reserva_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("reserva_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("ota_codigo_confirmacao", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ota_nome_convidado", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("total_pago_ota", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("taxa_comissao", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("valor_liquido_recebido", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ota_solicitacoes_especificas", "STRING", mode="NULLABLE"),

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
                -- 1. Seleciona as colunas (sem conversão de tipo necessária)
                ota_reserva_id,
                reserva_id,
                ota_codigo_confirmacao,
                ota_nome_convidado,
                total_pago_ota,
                taxa_comissao,
                valor_liquido_recebido,
                ota_solicitacoes_especificas,
                
                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY ota_reserva_id -- CHAVE da tabela
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
        ON T.ota_reserva_id = S.ota_reserva_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'ota_reserva_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                T.reserva_id = S.reserva_id,
                T.ota_codigo_confirmacao = S.ota_codigo_confirmacao,
                T.ota_nome_convidado = S.ota_nome_convidado,
                T.total_pago_ota = S.total_pago_ota,
                T.taxa_comissao = S.taxa_comissao,
                T.valor_liquido_recebido = S.valor_liquido_recebido,
                T.ota_solicitacoes_especificas = S.ota_solicitacoes_especificas,
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'ota_reserva_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                ota_reserva_id, reserva_id, ota_codigo_confirmacao, ota_nome_convidado,
                total_pago_ota, taxa_comissao, valor_liquido_recebido,
                ota_solicitacoes_especificas,
                insert_date, update_date
            )
            VALUES (
                S.ota_reserva_id, S.reserva_id, S.ota_codigo_confirmacao, S.ota_nome_convidado,
                S.total_pago_ota, S.taxa_comissao, S.valor_liquido_recebido,
                S.ota_solicitacoes_especificas,
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