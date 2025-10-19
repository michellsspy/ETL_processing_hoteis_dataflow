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


class ProcessTrustedHoteis(beam.DoFn):
    """
    Este DoFn processa a tabela raw_hoteis para a camada trusted.
    
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
        self.table_name = "hoteis" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedHoteis.")
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
            bigquery.SchemaField("hotel_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("nome_hotel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("endereco", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cidade", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estado", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estrelas", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("numero_quartos", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("comodidades", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("telefone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email_contato", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data_abertura", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("horario_checkin", "TIME", mode="NULLABLE"), # STRING -> TIME
            bigquery.SchemaField("horario_checkout", "TIME", mode="NULLABLE"), # STRING -> TIME
            bigquery.SchemaField("categoria_hotel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tipo_hotel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ano_fundacao", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("capacidade_total", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("possui_acessibilidade", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("certificacoes", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("descricao_hotel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("numero_funcionarios", "INTEGER", mode="NULLABLE"),

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
                hotel_id,
                nome_hotel,
                endereco,
                cidade,
                estado,
                estrelas,
                numero_quartos,
                comodidades,
                telefone,
                email_contato,
                
                -- 2. Validação/Conversão de Tipos
                SAFE.PARSE_DATE('%Y-%m-%d', data_abertura) AS data_abertura,
                SAFE.PARSE_TIME('%H:%M:%S', horario_checkin) AS horario_checkin,
                SAFE.PARSE_TIME('%H:%M:%S', horario_checkout) AS horario_checkout,
                
                categoria_hotel,
                tipo_hotel,
                ano_fundacao,
                capacidade_total,
                possui_acessibilidade,
                certificacoes,
                latitude,
                longitude,
                descricao_hotel,
                numero_funcionarios,

                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY hotel_id -- CHAVE da tabela
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
        ON T.hotel_id = S.hotel_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'hotel_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                T.nome_hotel = S.nome_hotel,
                T.endereco = S.endereco,
                T.cidade = S.cidade,
                T.estado = S.estado,
                T.estrelas = S.estrelas,
                T.numero_quartos = S.numero_quartos,
                T.comodidades = S.comodidades,
                T.telefone = S.telefone,
                T.email_contato = S.email_contato,
                T.data_abertura = S.data_abertura,
                T.horario_checkin = S.horario_checkin,
                T.horario_checkout = S.horario_checkout,
                T.categoria_hotel = S.categoria_hotel,
                T.tipo_hotel = S.tipo_hotel,
                T.ano_fundacao = S.ano_fundacao,
                T.capacidade_total = S.capacidade_total,
                T.possui_acessibilidade = S.possui_acessibilidade,
                T.certificacoes = S.certificacoes,
                T.latitude = S.latitude,
                T.longitude = S.longitude,
                T.descricao_hotel = S.descricao_hotel,
                T.numero_funcionarios = S.numero_funcionarios,
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'hotel_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                hotel_id, nome_hotel, endereco, cidade, estado, estrelas,
                numero_quartos, comodidades, telefone, email_contato, data_abertura,
                horario_checkin, horario_checkout, categoria_hotel, tipo_hotel,
                ano_fundacao, capacidade_total, possui_acessibilidade, certificacoes,
                latitude, longitude, descricao_hotel, numero_funcionarios,
                insert_date, update_date
            )
            VALUES (
                S.hotel_id, S.nome_hotel, S.endereco, S.cidade, S.estado, S.estrelas,
                S.numero_quartos, S.comodidades, S.telefone, S.email_contato, S.data_abertura,
                S.horario_checkin, S.horario_checkout, S.categoria_hotel, S.tipo_hotel,
                S.ano_fundacao, S.capacidade_total, S.possui_acessibilidade, S.certificacoes,
                S.latitude, S.longitude, S.descricao_hotel, S.numero_funcionarios,
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