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


class ProcessTrustedHospedes(beam.DoFn):
    """
    Este DoFn processa a tabela raw_hospedes para a camada trusted.
    
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
        self.table_name = "hospedes" # Nome base da tabela
        self.bq_client = None

    def setup(self):
        """
        Inicializa o cliente BigQuery no worker.
        """
        try:
            self.bq_client = bigquery.Client(project=self.projeto)
            logger.info("Cliente BigQuery inicializado em ProcessTrustedHospedes.")
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
            bigquery.SchemaField("hospede_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("nome_completo", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cpf", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data_nascimento", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("telefone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estado", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("nacionalidade", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data_cadastro", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("programa_fidelidade", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("profissao", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tipo_documento", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("numero_documento", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("empresa", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("eh_viajante_frequente", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("preferencias_hospede", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("restricoes_alimentares", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data_ultima_hospedagem", "DATE", mode="NULLABLE"), # STRING -> DATE
            bigquery.SchemaField("total_hospedagens", "INTEGER", mode="NULLABLE"),

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
                hospede_id,
                nome_completo,
                cpf,
                
                -- 2. Validação/Conversão de Tipos
                SAFE.PARSE_DATE('%Y-%m-%d', data_nascimento) AS data_nascimento,
                email,
                telefone,
                estado,
                nacionalidade,
                SAFE.PARSE_DATE('%Y-%m-%d', data_cadastro) AS data_cadastro,
                programa_fidelidade,
                profissao,
                tipo_documento,
                numero_documento,
                empresa,
                eh_viajante_frequente,
                preferencias_hospede,
                restricoes_alimentares,
                SAFE.PARSE_DATE('%Y-%m-%d', data_ultima_hospedagem) AS data_ultima_hospedagem,
                total_hospedagens,

                -- 3. Define as novas colunas de auditoria
                CURRENT_DATETIME() AS insert_date,
                CAST(NULL AS DATETIME) AS update_date
            
            FROM (
                -- 4. Subquery interna para Deduplicação
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY hospede_id -- CHAVE da tabela
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
        ON T.hospede_id = S.hospede_id -- A Chave de Negócio (PK)

        -- 6. Lógica de Atualização (Se o 'hospede_id' JÁ EXISTIR na trusted)
        WHEN MATCHED THEN
            UPDATE SET
                T.nome_completo = S.nome_completo,
                T.cpf = S.cpf,
                T.data_nascimento = S.data_nascimento,
                T.email = S.email,
                T.telefone = S.telefone,
                T.estado = S.estado,
                T.nacionalidade = S.nacionalidade,
                T.data_cadastro = S.data_cadastro,
                T.programa_fidelidade = S.programa_fidelidade,
                T.profissao = S.profissao,
                T.tipo_documento = S.tipo_documento,
                T.numero_documento = S.numero_documento,
                T.empresa = S.empresa,
                T.eh_viajante_frequente = S.eh_viajante_frequente,
                T.preferencias_hospede = S.preferencias_hospede,
                T.restricoes_alimentares = S.restricoes_alimentares,
                T.data_ultima_hospedagem = S.data_ultima_hospedagem,
                T.total_hospedagens = S.total_hospedagens,
                T.update_date = CURRENT_DATETIME() 

        -- 7. Lógica de Inserção (Se o 'hospede_id' NÃO EXISTIR na trusted)
        WHEN NOT MATCHED THEN
            INSERT (
                hospede_id, nome_completo, cpf, data_nascimento, email,
                telefone, estado, nacionalidade, data_cadastro, programa_fidelidade,
                profissao, tipo_documento, numero_documento, empresa,
                eh_viajante_frequente, preferencias_hospede, restricoes_alimentares,
                data_ultima_hospedagem, total_hospedagens,
                insert_date, update_date
            )
            VALUES (
                S.hospede_id, S.nome_completo, S.cpf, S.data_nascimento, S.email,
                S.telefone, S.estado, S.nacionalidade, S.data_cadastro, S.programa_fidelidade,
                S.profissao, S.tipo_documento, S.numero_documento, S.empresa,
                S.eh_viajante_frequente, S.preferencias_hospede, S.restricoes_alimentares,
                S.data_ultima_hospedagem, S.total_hospedagens,
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