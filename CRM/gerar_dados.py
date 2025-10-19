import random
import names
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

# --- Variáveis de Configuração ---
# O bucket que você mencionou
GCS_BUCKET_PATH = "gs://bk-etl-hotelaria" 
# ----------------------------------


# Inicializa o Faker
fake = Faker('pt_BR')
print("Faker e Pandas inicializados.")

# Célula 2: Funções de Geração - HOTEIS (Adaptada para Pandas)
def gerar_hoteis(num_hoteis=50):
    COMODIDADES = ['Wi-Fi Grátis', 'Piscina', 'Academia', 'Spa', 'Estacionamento', 'Restaurante', 'Bar', 'Serviço de Quarto', 'Centro de Convenções', 'Pet Friendly']
    PREFIXOS = ['Grand', 'Royal', 'Plaza', 'Imperial', 'Golden', 'Paradise', 'Ocean']
    SUFIXOS = ['Hotel', 'Resort', 'Palace', 'Inn', 'Suites']
    CATEGORIAS_HOTEL = ['Econômico', 'Médio', 'Superior', 'Luxo', 'Boutique', 'Business']
    TIPOS_HOTEL = ['Urbano', 'Praia', 'Montanha', 'Campo', 'Histórico', 'Familiar']
    
    hoteis_data = []
    # Definindo as colunas com base no seu Schema original
    colunas_hoteis = [
        "hotel_id", "nome_hotel", "endereco", "cidade", "estado", "estrelas", 
        "numero_quartos", "comodidades", "telefone", "email_contato", 
        "data_abertura", "horario_checkin", "horario_checkout", 
        "categoria_hotel", "tipo_hotel", "ano_fundacao", "capacidade_total", 
        "possui_acessibilidade", "certificacoes", "latitude", "longitude", 
        "descricao_hotel", "numero_funcionarios"
    ]

    for i in range(num_hoteis):
        nome_hotel = f"{random.choice(PREFIXOS)} {fake.city_suffix()} {random.choice(SUFIXOS)}"
        num_comodidades = random.randint(3, 7)
        comodidades_selecionadas = random.sample(COMODIDADES, num_comodidades)
        
        ano_fundacao = random.randint(1980, 2020)
        capacidade_total = random.randint(100, 500)
        possui_acessibilidade = random.choice([True, False])
        certificacoes = random.sample(['ISO 9001', 'Sustentável', 'Green Key', 'TripAdvisor Excellence'], random.randint(0, 2))
        
        hotel = (
            101 + i, nome_hotel, fake.street_address(), fake.city(), fake.state_abbr(),
            random.randint(3, 5), random.randint(50, 250),
            ', '.join(comodidades_selecionadas), fake.phone_number(),
            f"contato@{nome_hotel.lower().split(' ')[0]}.com",
            fake.past_date(start_date="-20y"), "14:00", "12:00",
            random.choice(CATEGORIAS_HOTEL), random.choice(TIPOS_HOTEL),
            ano_fundacao, capacidade_total, possui_acessibilidade,
            ', '.join(certificacoes), round(random.uniform(-23.5, -23.7), 6),
            round(random.uniform(-46.6, -46.8), 6), fake.text(max_nb_chars=200),
            random.randint(50, 200)
        )
        hoteis_data.append(hotel)
        
    # Cria o DataFrame Pandas
    df = pd.DataFrame(hoteis_data, columns=colunas_hoteis)
    # Converte colunas de data para garantir o formato correto no CSV
    df['data_abertura'] = pd.to_datetime(df['data_abertura']).dt.date
    return df

# Célula 3: Funções de Geração - QUARTOS (Adaptada para Pandas)
def gerar_quartos(df_hoteis):
    # (Lógica de geração de dados original... igual à sua)
    TIPOS_QUARTO = {
        'Standard': {'capacidade': [1, 2], 'percentual': 0.50, 'preco_base': 250.0},
        'Superior': {'capacidade': [2, 3], 'percentual': 0.25, 'preco_base': 400.0},
        'Suíte': {'capacidade': [2, 4], 'percentual': 0.15, 'preco_base': 650.0},
        'Suíte Presidencial': {'capacidade': [2, 5], 'percentual': 0.10, 'preco_base': 1200.0}
    }
    VISTAS = ['Cidade', 'Piscina', 'Mar', 'Jardim', 'Interna']
    COMODIDADES_QUARTO = ['Frigobar', 'Varanda', 'Banheira', 'Cofre', 'Smart TV', 'Ar Condicionado Central', 'Cafeteira', 'Secador de Cabelo']
    TAMANHOS_QUARTO = ['Pequeno (20m²)', 'Médio (30m²)', 'Grande (45m²)', 'Amplo (60m²+)']
    
    quartos_data = []
    quarto_id_global = 1001
    # Coleta informações dos hoteis (Pandas)
    hoteis_info = df_hoteis.to_dict('records') 
    
    colunas_quartos = [
        "quarto_id", "hotel_id", "numero_quarto", "tipo_quarto", 
        "capacidade_maxima", "preco_diaria_base", "andar", "vista", 
        "comodidades_quarto", "possui_ar_condicionado", "tamanho_quarto", 
        "status_manutencao", "ultima_manutencao", "eh_smoke_free", 
        "possui_kit_boas_vindas", "numero_camas"
    ]
    
    for hotel in hoteis_info:
        total_quartos_hotel = hotel['numero_quartos']
        
        tipos_dist = []
        tipos_count = {tipo: int(total_quartos_hotel * config['percentual']) for tipo, config in TIPOS_QUARTO.items()}
        total_distribuido = sum(tipos_count.values())
        tipos_count['Standard'] += total_quartos_hotel - total_distribuido 
        
        for tipo, count in tipos_count.items():
            tipos_dist.extend([tipo] * count)
        
        random.shuffle(tipos_dist)
            
        for i in range(total_quartos_hotel):
            tipo_quarto_atual = tipos_dist[i]
            config = TIPOS_QUARTO[tipo_quarto_atual]
            preco_final = config['preco_base'] * (1 + (hotel['estrelas'] - 3) * 0.15)
            
            andar = (i // 20) + 1
            numero_quarto_str = f"{andar:02d}{(i % 20) + 1:02d}"
            
            num_comodidades_qto = random.randint(2, len(COMODIDADES_QUARTO))
            comodidades_qto_selecionadas = random.sample(COMODIDADES_QUARTO, num_comodidades_qto)
            
            possui_ar_condicionado = 'Ar Condicionado Central' in comodidades_qto_selecionadas
            tamanho_quarto = random.choice(TAMANHOS_QUARTO)
            status_manutencao = random.choices(['Disponível', 'Em Manutenção', 'Em Limpeza'], weights=[0.85, 0.05, 0.10], k=1)[0]
            ultima_manutencao = fake.past_date(start_date="-180d") if random.random() < 0.7 else None
            eh_smoke_free = random.choice([True, False])
            possui_kit_boas_vindas = random.choice([True, False])
            
            quarto = (
                quarto_id_global, hotel['hotel_id'], numero_quarto_str,
                tipo_quarto_atual, random.randint(config['capacidade'][0], config['capacidade'][1]),
                round(preco_final, 2), andar, random.choice(VISTAS),
                ', '.join(comodidades_qto_selecionadas), possui_ar_condicionado,
                tamanho_quarto, status_manutencao, ultima_manutencao,
                eh_smoke_free, possui_kit_boas_vindas, random.randint(1, 4)
            )
            quartos_data.append(quarto)
            quarto_id_global += 1
            
    df = pd.DataFrame(quartos_data, columns=colunas_quartos)
    df['ultima_manutencao'] = pd.to_datetime(df['ultima_manutencao']).dt.date
    return df

# Célula 4: Funções de Geração - HÓSPEDES (Adaptada para Pandas)
def gerar_hospedes(num_hospedes=8000):
    # (Lógica de geração de dados original... igual à sua)
    PROGRAMAS_FIDELIDADE = ['Bronze', 'Prata', 'Gold', 'Platinum']
    PROFISSOES = ['Engenheiro', 'Médico', 'Professor', 'Empresário', 'Estudante', 'Aposentado', 'Artista', 'Tecnologia', 'Comércio', 'Servidor Público', None]
    TIPOS_DOCUMENTO = ['CPF', 'Passaporte', 'RG', 'CNH']
    
    hospedes_data = []
    colunas_hospedes = [
        "hospede_id", "nome_completo", "cpf", "data_nascimento", "email", 
        "telefone", "estado", "nacionalidade", "data_cadastro", 
        "programa_fidelidade", "profissao", "tipo_documento", 
        "numero_documento", "empresa", "eh_viajante_frequente", 
        "preferencias_hospede", "restricoes_alimentares", 
        "data_ultima_hospedagem", "total_hospedagens"
    ]

    for i in range(num_hospedes):
        nome = fake.name()
        data_nasc = fake.date_of_birth(minimum_age=18, maximum_age=85)
        data_cadastro_start = data_nasc + timedelta(days=18*365)
        data_cadastro = fake.date_time_between(start_date=data_cadastro_start, end_date="now").date()
        
        profissao = random.choice(PROFISSOES)
        tipo_documento = random.choice(TIPOS_DOCUMENTO)
        numero_documento = fake.cpf() if tipo_documento == 'CPF' else fake.license_plate().replace('-', '')
        empresa = fake.company() if random.random() < 0.4 else None
        eh_viajante_frequente = random.choice([True, False])
        preferencias = random.sample(['Quieto', 'Café da manhã', 'Jornal', 'Toalhas extras', 'Travesseiro especial'], random.randint(0, 3))
        restricoes_alimentares = random.choice([None, 'Vegetariano', 'Vegano', 'Sem glúten', 'Sem lactose'])
        data_ultima_hospedagem = fake.past_date(start_date="-365d") if random.random() < 0.6 else None
        
        hospede = (
            2001 + i, nome, fake.cpf(), data_nasc,
            f"{nome.split(' ')[0].lower()}_{i}@email.com",
            fake.phone_number(), fake.state_abbr(), 'Brasil', data_cadastro,
            random.choice(PROGRAMAS_FIDELIDADE), profissao, tipo_documento,
            numero_documento, empresa, eh_viajante_frequente,
            ', '.join(preferencias) if preferencias else None,
            restricoes_alimentares, data_ultima_hospedagem,
            random.randint(0, 50)
        )
        hospedes_data.append(hospede)
        
    df = pd.DataFrame(hospedes_data, columns=colunas_hospedes)
    df['data_nascimento'] = pd.to_datetime(df['data_nascimento']).dt.date
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.date
    df['data_ultima_hospedagem'] = pd.to_datetime(df['data_ultima_hospedagem']).dt.date
    return df

# Célula 5: Funções de Geração - DEPENDENTES (Adaptada para Pandas)
def gerar_dependentes(df_hoteis, df_quartos, df_hospedes, num_reservas=15000):
    # (Lógica de geração de dados original... igual à sua)
    CANAIS = ['Website Hotel', 'Booking.com', 'Expedia', 'Telefone', 'Balcão', 'Agência de Viagem']
    OTAS = ['Booking.com', 'Expedia'] 
    SERVICOS = [
        ('Restaurante - Jantar', 120.0), ('Restaurante - Bebida', 15.0),
        ('Bar - Coquetel', 35.0), ('Serviço de Quarto - Lanche', 50.0),
        ('Frigobar - Refrigerante', 8.0), ('Frigobar - Água', 6.0),
        ('Massagem Relaxante', 250.0), ('Tratamento Facial', 180.0),
        ('Lavanderia - Peça', 20.0), ('Estacionamento - Diária', 40.0),
        ('Translado Aeroporto', 80.0), ('Passeio Guiado', 150.0),
        ('Aluguel de Carro', 200.0)
    ]
    FIDELIDADE_DESCONTOS = {'Bronze': 0.0, 'Prata': 0.05, 'Gold': 0.10, 'Platinum': 0.15}
    FORMAS_PAGAMENTO = ['Cartão de Crédito', 'Pix', 'Dinheiro', 'Cartão de Débito', 'Transferência Bancária']
    SOLICITACOES = ['', 'Berço', 'Andar alto', 'Check-in antecipado', 'Quarto para não fumantes', 'Cama casal', 'Vista mar', 'Quarto térreo']
    MOTIVOS_VIAGEM = ['Lazer', 'Negócios', 'Família', 'Romance', 'Evento', 'Saúde']
    
    HOJE = datetime.now().date()
    TAXA_IMPOSTO = 0.05
    
    # Coletando dados dos DFs Pandas
    quartos_info = df_quartos[["quarto_id", "hotel_id", "preco_diaria_base", "capacidade_maxima"]].to_dict('records')
    hospedes_info = df_hospedes[["hospede_id", "programa_fidelidade"]].to_dict('records')

    reservas_data = []
    consumos_data = []
    faturas_data = []
    reservas_ota_data = [] 
    
    # Colunas
    colunas_reservas = [
        "reserva_id", "hospede_id", "quarto_id", "hotel_id", "data_reserva", 
        "data_checkin", "data_checkout", "numero_noites", "numero_adultos", 
        "numero_criancas", "canal_reserva", "status_reserva", 
        "data_cancelamento", "solicitacoes_especiais", "valor_total_estadia", 
        "motivo_viagem", "motivo_cancelamento", "taxa_limpeza", "taxa_turismo", 
        "avaliacao_hospede", "comentarios_hospede"
    ]
    colunas_consumos = [
        "consumo_id", "reserva_id", "hospede_id", "hotel_id", "nome_servico", 
        "data_consumo", "quantidade", "valor_total_consumo", "hora_consumo", 
        "local_consumo", "funcionario_responsavel"
    ]
    colunas_faturas = [
        "fatura_id", "reserva_id", "hospede_id", "data_emissao", 
        "data_vencimento", "status_pagamento", "forma_pagamento", 
        "subtotal_estadia", "subtotal_consumos", "descontos", "impostos", 
        "valor_total", "data_pagamento", "taxa_limpeza", "taxa_turismo", 
        "taxa_servico", "numero_transacao"
    ]
    colunas_reservas_ota = [
        "ota_reserva_id", "reserva_id", "ota_codigo_confirmacao", 
        "ota_nome_convidado", "total_pago_ota", "taxa_comissao", 
        "valor_liquido_recebido", "ota_solicitacoes_especificas"
    ]
    
    consumo_id_global = 50001
    fatura_id_global = 9001
    ota_reserva_id_global = 7001 

    for i in range(num_reservas):
        reserva_id = 10001 + i
        quarto_selecionado = random.choice(quartos_info)
        hospede_selecionado = random.choice(hospedes_info)
        hospede_id = hospede_selecionado['hospede_id']
        
        # (Lógica de datas, status, etc... igual à sua)
        dias_antecedencia = random.randint(1, 90)
        data_reserva = datetime.now() - timedelta(days=dias_antecedencia + random.randint(5, 365*2))
        data_checkin = data_reserva + timedelta(days=dias_antecedencia)
        num_noites = random.randint(1, 10)
        data_checkout = data_checkin + timedelta(days=num_noites)

        if data_checkout.date() < HOJE:
            status_reserva = 'Concluída'
        elif data_checkin.date() < HOJE and data_checkout.date() > HOJE:
            status_reserva = 'Hospedado' 
        elif data_checkin.date() > HOJE:
            status_reserva = random.choices(['Confirmada', 'Cancelada'], weights=[0.9, 0.1], k=1)[0]
        else:
             status_reserva = 'Concluída'
        
        data_cancelamento = None
        motivo_cancelamento = None
        if status_reserva == 'Cancelada':
            dias_para_cancelar = random.randint(1, dias_antecedencia - 1 if dias_antecedencia > 1 else 1)
            data_cancelamento = data_reserva + timedelta(days=dias_para_cancelar)
            motivo_cancelamento = random.choice(['Plano alterado', 'Problemas de saúde', 'Problemas financeiros', 'Encontrou opção melhor', 'Motivos pessoais'])

        num_adultos = random.randint(1, quarto_selecionado['capacidade_maxima'])
        capacidade_restante = quarto_selecionado['capacidade_maxima'] - num_adultos
        num_criancas = random.randint(0, capacidade_restante)

        valor_base_estadia = num_noites * quarto_selecionado['preco_diaria_base']
        taxa_limpeza = round(valor_base_estadia * 0.02, 2)
        taxa_turismo = round(valor_base_estadia * 0.03, 2)
        canal_reserva = random.choice(CANAIS)
        
        reservas_data.append((
            reserva_id, hospede_id, quarto_selecionado['quarto_id'], quarto_selecionado['hotel_id'],
            data_reserva.date(), data_checkin.date(), data_checkout.date(), num_noites,
            num_adultos, num_criancas, canal_reserva, status_reserva,
            data_cancelamento.date() if data_cancelamento else None,
            random.choice(SOLICITACOES), valor_base_estadia,
            random.choice(MOTIVOS_VIAGEM), motivo_cancelamento,
            taxa_limpeza, taxa_turismo,
            round(random.uniform(4.0, 5.0), 1) if status_reserva == 'Concluída' and random.random() < 0.7 else None,
            fake.text(max_nb_chars=100) if status_reserva == 'Concluída' and random.random() < 0.3 else None
        ))
        
        # --- GERAÇÃO DE DADOS RESERVAS_OTA ---
        if canal_reserva in OTAS and status_reserva != 'Cancelada':
            # (Lógica original... igual à sua)
            ota_reserva_id = ota_reserva_id_global
            ota_codigo_confirmacao = f"{canal_reserva[0:3].upper()}-{fake.pystr(8, 8, '0123456789ABCDEF')}"
            taxa_comissao = round(random.uniform(0.12, 0.25), 2)
            total_pago_ota = valor_base_estadia + taxa_limpeza + taxa_turismo
            valor_liquido_recebido = round(total_pago_ota * (1 - taxa_comissao), 2)
            ota_nome_convidado = fake.name()
            ota_solicitacoes_especificas = fake.text(max_nb_chars=50) if random.random() < 0.3 else None
            
            reservas_ota_data.append((
                ota_reserva_id, reserva_id, ota_codigo_confirmacao,
                ota_nome_convidado, round(total_pago_ota, 2),
                taxa_comissao, valor_liquido_recebido,
                ota_solicitacoes_especificas
            ))
            ota_reserva_id_global += 1

        # --- Consumos e Faturas (Lógica original) ---
        subtotal_consumos = 0.0
        if status_reserva in ['Concluída', 'Hospedado']:
            num_consumos_reserva = random.randint(0, 5)
            for _ in range(num_consumos_reserva):
                # (Lógica original... igual à sua)
                servico_selecionado = random.choice(SERVICOS)
                servico_nome = servico_selecionado[0]
                servico_preco_base = servico_selecionado[1]
                quantidade = random.randint(1, 3)
                valor_consumo_total = round(servico_preco_base * quantidade, 2)
                dias_na_estadia = random.randint(0, num_noites -1 if num_noites > 0 else 0)
                data_consumo = data_checkin + timedelta(days=dias_na_estadia)
                hora_consumo = f"{random.randint(6, 23):02d}:{random.randint(0, 59):02d}"
                
                consumos_data.append((
                    consumo_id_global, reserva_id, hospede_id, quarto_selecionado['hotel_id'],
                    servico_nome, data_consumo.date(), quantidade,
                    valor_consumo_total, hora_consumo,
                    random.choice(['Quarto', 'Restaurante', 'Bar', 'Spa', 'Balcão']),
                    fake.name() if random.random() < 0.5 else None
                ))
                subtotal_consumos += valor_consumo_total
                consumo_id_global += 1

            if status_reserva == 'Concluída':
                # (Lógica original... igual à sua)
                data_emissao = data_checkout + timedelta(days=1)
                fator_desconto = FIDELIDADE_DESCONTOS.get(hospede_selecionado['programa_fidelidade'], 0.0)
                desconto_valor = round(valor_base_estadia * fator_desconto, 2)
                subtotal_pos_desconto = (valor_base_estadia + subtotal_consumos + taxa_limpeza + taxa_turismo) - desconto_valor
                impostos_valor = round(subtotal_pos_desconto * TAXA_IMPOSTO, 2)
                valor_total_fatura = subtotal_pos_desconto + impostos_valor
                status_pagamento = random.choices(['Pago', 'Pendente', 'Atrasado'], weights=[0.85, 0.10, 0.05], k=1)[0]
                data_pagamento = data_emissao + timedelta(days=random.randint(0, 5)) if status_pagamento == 'Pago' else None
                
                faturas_data.append((
                    fatura_id_global, reserva_id, hospede_id, 
                    data_emissao.date(), data_emissao.date() + timedelta(days=15),
                    status_pagamento, random.choice(FORMAS_PAGAMENTO),
                    valor_base_estadia, subtotal_consumos, desconto_valor,
                    impostos_valor, valor_total_fatura, data_pagamento,
                    taxa_limpeza, taxa_turismo,
                    round(random.uniform(0.0, 0.10), 4), fake.iban()
                ))
                fatura_id_global += 1

    # Criar DataFrames Pandas
    df_reservas = pd.DataFrame(reservas_data, columns=colunas_reservas)
    df_consumos = pd.DataFrame(consumos_data, columns=colunas_consumos)
    df_faturas = pd.DataFrame(faturas_data, columns=colunas_faturas)
    df_reservas_ota = pd.DataFrame(reservas_ota_data, columns=colunas_reservas_ota)

    # Converter colunas de data
    for col in ['data_reserva', 'data_checkin', 'data_checkout', 'data_cancelamento']:
        df_reservas[col] = pd.to_datetime(df_reservas[col]).dt.date
    df_consumos['data_consumo'] = pd.to_datetime(df_consumos['data_consumo']).dt.date
    for col in ['data_emissao', 'data_vencimento', 'data_pagamento']:
        df_faturas[col] = pd.to_datetime(df_faturas[col]).dt.date
        
    return df_reservas, df_consumos, df_faturas, df_reservas_ota

# Célula 6: Execução Principal (Adaptada para Pandas e GCS)
def run_generation_to_gcs():
    
    # Define o caminho completo do bucket
    # Vamos salvar em subpastas para organizar
    schema_path = "transient" 
    
    print("--- Gerando Hoteis ---")
    df_hoteis = gerar_hoteis(num_hoteis=50)
    path_hoteis = f"{GCS_BUCKET_PATH}/{schema_path}/source_hoteis/source_hoteis.csv"
    df_hoteis.to_csv(path_hoteis, index=False, encoding='utf-8')
    print(f"{len(df_hoteis)} registros de hoteis salvos em {path_hoteis}")

    print("\n--- Gerando Quartos ---")
    df_quartos = gerar_quartos(df_hoteis)
    path_quartos = f"{GCS_BUCKET_PATH}/{schema_path}/source_quartos/source_quartos.csv"
    df_quartos.to_csv(path_quartos, index=False, encoding='utf-8')
    print(f"{len(df_quartos)} registros de quartos salvos em {path_quartos}")

    print("\n--- Gerando Hóspedes ---")
    df_hospedes = gerar_hospedes(num_hospedes=8000)
    path_hospedes = f"{GCS_BUCKET_PATH}/{schema_path}/source_hospedes/source_hospedes.csv"
    df_hospedes.to_csv(path_hospedes, index=False, encoding='utf-8')
    print(f"{len(df_hospedes)} registros de hóspedes salvos em {path_hospedes}")

    print("\n--- Gerando Reservas, Consumos, Faturas e Reservas OTA ---")
    df_reservas, df_consumos, df_faturas, df_reservas_ota = gerar_dependentes(
        df_hoteis, df_quartos, df_hospedes, 
        num_reservas=15000
    )
    
    path_reservas = f"{GCS_BUCKET_PATH}/{schema_path}/source_reservas/source_reservas.csv"
    df_reservas.to_csv(path_reservas, index=False, encoding='utf-8')
    print(f"{len(df_reservas)} registros de reservas salvos em {path_reservas}")
    
    path_consumos = f"{GCS_BUCKET_PATH}/{schema_path}/source_consumos/source_consumos.csv"
    df_consumos.to_csv(path_consumos, index=False, encoding='utf-8')
    print(f"{len(df_consumos)} registros de consumos salvos em {path_consumos}")

    path_faturas = f"{GCS_BUCKET_PATH}/{schema_path}/source_faturas/source_faturas.csv"
    df_faturas.to_csv(path_faturas, index=False, encoding='utf-8')
    print(f"{len(df_faturas)} registros de faturas salvos em {path_faturas}")
    
    path_reservas_ota = f"{GCS_BUCKET_PATH}/{schema_path}/source_reservas_ota/source_reservas_ota.csv"
    df_reservas_ota.to_csv(path_reservas_ota, index=False, encoding='utf-8')
    print(f"{len(df_reservas_ota)} registros de reservas OTA salvos em {path_reservas_ota}")
    
    print("\n--- Processo de Geração Concluído! Arquivos salvos no GCS. ---")

# Executa a função principal
if __name__ == "__main__":
    run_generation_to_gcs()