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


class ProcessTrustedQuartos(beam.DoFn):
    """
    Este DoFn processa a tabela raw_quartos para a camada trusted.
    
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
        self.table_name = "quartos" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedQuartos.")
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
            bigquery.SchemaField("quarto_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("hotel_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("numero_quarto", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("tipo_quarto", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("capacidade_maxima", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("preco_diaria_base", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("andar", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("vista", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("comodidades_quarto", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("possui_ar_condicionado", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("tamanho_quarto", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status_manutencao", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ultima_manutencao", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("eh_smoke_free", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("possui_kit_boas_vindas", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("numero_camas", "INTEGER", mode="NULLABLE"),

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
                quarto_id,
                hotel_id,
                numero_quarto,
                tipo_quarto,
                capacidade_maxima,
                preco_diaria_base,
                andar,
                vista,
                comodidades_quarto,
                possui_ar_condicionado,
                tamanho_quarto,
                status_manutencao,
                
                -- 2. Validação/Conversão de Tipos
                SAFE.PARSE_DATE('%Y-%m-%d', ultima_manutencao) AS ultima_manutencao,
                
                eh_smoke_free,
                possui_kit_boas_vindas,
                numero_camas,

                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY quarto_id -- CHAVE da tabela
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
        ON T.quarto_id = S.quarto_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'quarto_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                T.hotel_id = S.hotel_id,
                T.numero_quarto = S.numero_quarto,
                T.tipo_quarto = S.tipo_quarto,
                T.capacidade_maxima = S.capacidade_maxima,
                T.preco_diaria_base = S.preco_diaria_base,
                T.andar = S.andar,
                T.vista = S.vista,
                T.comodidades_quarto = S.comodidades_quarto,
                T.possui_ar_condicionado = S.possui_ar_condicionado,
                T.tamanho_quarto = S.tamanho_quarto,
                T.status_manutencao = S.status_manutencao,
                T.ultima_manutencao = S.ultima_manutencao,
                T.eh_smoke_free = S.eh_smoke_free,
                T.possui_kit_boas_vindas = S.possui_kit_boas_vindas,
                T.numero_camas = S.numero_camas,
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'quarto_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                quarto_id, hotel_id, numero_quarto, tipo_quarto, capacidade_maxima,
                preco_diaria_base, andar, vista, comodidades_quarto,
                possui_ar_condicionado, tamanho_quarto, status_manutencao,
                ultima_manutencao, eh_smoke_free, possui_kit_boas_vindas, numero_camas,
                insert_date, update_date
            )
            VALUES (
                S.quarto_id, S.hotel_id, S.numero_quarto, S.tipo_quarto, S.capacidade_maxima,
                S.preco_diaria_base, S.andar, S.vista, S.comodidades_quarto,
                S.possui_ar_condicionado, S.tamanho_quarto, S.status_manutencao,
                S.ultima_manutencao, S.eh_smoke_free, S.possui_kit_boas_vindas, S.numero_camas,
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