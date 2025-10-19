FROM python:3.12-slim

# Copiando as dependências Beam e Template Launcher
COPY --from=apache/beam_python3.12_sdk:2.64.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python310-template-launcher-base:20230622_RC00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# Argumentos de build
ARG DB_USER 
ARG DB_PASSWORD
ARG DB_HOST
ARG DB_PORT
ARG DB_SERVICE

# Variáveis de ambiente
ENV DB_USER=${DB_USER}
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}
ENV DB_SERVICE=${DB_SERVICE}

# Variáveis específicas do template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=30
ENV FLEX_TEMPLATES_NUM_LOG_LINES=1000

# Copiando código
COPY . .

# Instalando dependências
RUN pip install --upgrade pip && pip install -U -r requirements.txt

RUN ls -la /opt/apache/beam && \
    ls -la /opt/google/dataflow/python_template_launcher && \
    ls -la ${WORKDIR}

# Entrypoint
ENTRYPOINT ["/opt/apache/beam/boot"]