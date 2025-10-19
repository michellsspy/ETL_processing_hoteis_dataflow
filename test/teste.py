folder_name = "source_reservas_teste"
partes = folder_name.split('_')  # Isso cria uma lista: ['source', 'reservas', 'teste']
nome_sem_prefixo = '_'.join(partes[1:]) # Isso junta a lista a partir do item 1: 'reservas_teste'
table_name = f"raw_{nome_sem_prefixo}" # Resultado: 'raw_reservas_teste'
print(table_name)