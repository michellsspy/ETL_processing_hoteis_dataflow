from google.cloud import secretmanager
import tempfile
import logging
import os

def get_secret():
    # Recuperando a secret
    PROJECT_ID = "992325149095"
    SECRET_NAME = "etl-hoteis"  # Nome do Secret salvo no Secret Manager

    """Recupera o secret do Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"

        response = client.access_secret_version(name=secret_path)
        secret_data = response.payload.data.decode("UTF-8")

        logging.info("\nSecret recuperado com sucesso.\n")
        return secret_data
    except Exception as e:
        logging.error(f"\nErro ao acessar o Secret Manager: {e}\n", exc_info=True)
        raise

def save_secret_to_temp_file(secret_data):
    """Salva o secret em um arquivo temporário com permissões restritas."""
    try:
        # Cria um arquivo temporário com um nome único
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as temp_file:
            temp_file.write(secret_data)
            temp_path = temp_file.name

        # Aplica permissões restritas ao arquivo
        os.chmod(temp_path, 0o600)
        logging.info(f"Secret salvo temporariamente em {temp_path} com permissões 0o600.")
        return temp_path
    except Exception as e:
        logging.error(f"Erro ao salvar o secret em arquivo temporário: {e}", exc_info=True)
        raise