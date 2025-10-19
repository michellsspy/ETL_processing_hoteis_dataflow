from google.cloud import storage
 
def upload_log_to_gcs(logger,log_stream,data_now):
    """Salva os logs no GCS."""
    log_file = f"log_{data_now}.log"

    try:
        client = storage.Client()
        bucket_name = "site_prosite"
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"log/{log_file}")

        # Obtém os logs do buffer em memória
        log_content = log_stream.getvalue()

        # Faz o upload do conteúdo para o GCS
        blob.upload_from_string(log_content)
        logger.info(f"Log salvo em gs://{bucket_name}/log/{log_file}")
    except Exception as e:
        logger.error(f"Erro ao salvar log no GCS: {e}", exc_info=True)