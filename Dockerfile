FROM python:3.12-slim

# Copiando as dependências Beam e Template Launcher
COPY --from=apache/beam_python3.12_sdk:2.64.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python310-template-launcher-base:20230622_RC00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# Variáveis de ambiente FLEXÍVEIS
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=30
ENV FLEX_TEMPLATES_NUM_LOG_LINES=1000

# Copiando TODO o código
COPY . .

# Instalando dependências
RUN pip install --upgrade pip && pip install -U -r requirements.txt

# Verificação CRÍTICA da estrutura
RUN echo "🔍 Verificando estrutura do projeto..." && \
    echo "📁 Pipeline RAW:" && ls -la pipeline_hotelaria/raw/ && \
    echo "📁 Pipeline TRUSTED:" && ls -la pipeline_hotelaria/trusted/ && \
    echo "✅ Main files encontrados:" && \
    find . -name "main_*.py" -type f

# Entrypoint FLEXÍVEL
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]