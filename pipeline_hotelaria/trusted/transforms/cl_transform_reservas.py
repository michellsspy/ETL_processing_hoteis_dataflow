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


class ProcessTrustedReservas(beam.DoFn):
    """
    Este DoFn processa a tabela raw_reservas para a camada trusted.
    
    Ele executa uma query MERGE no BigQuery para:
    1. Buscar dados incrementais da camada raw.
    2. Corrigir os tipos de dados (STRING para DATE, FLOAT para DATE).
    3. Deduplicar os registros (mantendo o mais recente da raw).
    4. Inserir novos registros ou atualizar existentes na camada trusted.
    5. Gerenciar as colunas 'insert_date' e 'update_date' na trusted.
    """

    def __init__(self):
        self.projeto = "etl-hoteis"
        self.raw_dataset = "raw_hotelaria"
        self.trusted_dataset = "trusted_hotelaria"
        self.table_name = "reservas" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedReservas.")
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
            bigquery.SchemaField("reserva_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("hospede_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("quarto_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("hotel_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("data_reserva", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("data_checkin", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("data_checkout", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("numero_noites", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("numero_adultos", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("numero_criancas", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("canal_reserva", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_reserva", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data_cancelamento", "DATE", mode="NULLABLE"), # FLOAT -> DATE
            bigquery.SchemaField("solicitacoes_especiais", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("valor_total_estadia", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("motivo_viagem", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("motivo_cancelamento", "STRING", mode="NULLABLE"), # FLOAT -> STRING
            bigquery.SchemaField("taxa_limpeza", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("taxa_turismo", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("avaliacao_hospede", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("comentarios_hospede", "STRING", mode="NULLABLE"),

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
                reserva_id,
                hospede_id,
                quarto_id,
                hotel_id,
                
                -- 2. Validação/Conversão de Tipos
                SAFE.PARSE_DATE('%Y-%m-%d', data_reserva) AS data_reserva,
                SAFE.PARSE_DATE('%Y-%m-%d', data_checkin) AS data_checkin,
                SAFE.PARSE_DATE('%Y-%m-%d', data_checkout) AS data_checkout,
                numero_noites,
                numero_adultos,
                numero_criancas,
                canal_reserva,
                status_reserva,
                
                -- Converte FLOAT (datetime unix) para DATE.
                SAFE.DATE(DATETIME_SECONDS(CAST(data_cancelamento AS INT64))) AS data_cancelamento,
                
                solicitacoes_especiais,
                valor_total_estadia,
                motivo_viagem,
                
                -- Converte FLOAT para STRING
                CAST(motivo_cancelamento AS STRING) AS motivo_cancelamento,
                
                taxa_limpeza,
                taxa_turismo,
                avaliacao_hospede,
                comentarios_hospede,

                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY reserva_id -- CHAVE da tabela
                        ORDER BY insert_date DESC
                    ) as rn
                FROM
                    `{raw_table_id}`
                WHERE
                    -- 5. FILTRO INCREMENTAL
                    insert_date > DATETIME'{last_datetime}')
            )
            WHERE rn = 1 -- Pega apenas o registro mais recente (deduplicação)
            
        ) S
        ON T.reserva_id = S.reserva_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'reserva_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                T.hospede_id = S.hospede_id,
                T.quarto_id = S.quarto_id,
                T.hotel_id = S.hotel_id,
                T.data_reserva = S.data_reserva,
                T.data_checkin = S.data_checkin,
                T.data_checkout = S.data_checkout,
                T.numero_noites = S.numero_noites,
                T.numero_adultos = S.numero_adultos,
                T.numero_criancas = S.numero_criancas,
                T.canal_reserva = S.canal_reserva,
                T.status_reserva = S.status_reserva,
                T.data_cancelamento = S.data_cancelamento,
                T.solicitacoes_especiais = S.solicitacoes_especiais,
                T.valor_total_estadia = S.valor_total_estadia,
                T.motivo_viagem = S.motivo_viagem,
                T.motivo_cancelamento = S.motivo_cancelamento,
                T.taxa_limpeza = S.taxa_limpeza,
                T.taxa_turismo = S.taxa_turismo,
                T.avaliacao_hospede = S.avaliacao_hospede,
                T.comentarios_hospede = S.comentarios_hospede,
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'reserva_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                reserva_id, hospede_id, quarto_id, hotel_id, data_reserva,
                data_checkin, data_checkout, numero_noites, numero_adultos,
                numero_criancas, canal_reserva, status_reserva, data_cancelamento,
                solicitacoes_especiais, valor_total_estadia, motivo_viagem,
                motivo_cancelamento, taxa_limpeza, taxa_turismo, avaliacao_hospede,
                comentarios_hospede,
                insert_date, update_date
            )
            VALUES (
                S.reserva_id, S.hospede_id, S.quarto_id, S.hotel_id, S.data_reserva,
                S.data_checkin, S.data_checkout, S.numero_noites, S.numero_adultos,
                S.numero_criancas, S.canal_reserva, S.status_reserva, S.data_cancelamento,
                S.solicitacoes_especiais, S.valor_total_estadia, S.motivo_viagem,
                S.motivo_cancelamento, S.taxa_limpeza, S.taxa_turismo, S.avaliacao_hospede,
                S.comentarios_hospede,
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