# ==========================================================
# 🧩 BASE IMAGE OFICIAL — Dataflow Python 3.10 + Beam 2.64.0
# ==========================================================
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:beam-2.64.0

# ----------------------------------------------------------
# 📁 Diretório de trabalho
# ----------------------------------------------------------
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# ----------------------------------------------------------
# 📦 Copiar apenas requirements.txt primeiro (cache)
# ----------------------------------------------------------
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -U -r requirements.txt

# ----------------------------------------------------------
# 📂 Copiar o restante do código
# ----------------------------------------------------------
COPY . .

# ----------------------------------------------------------
# ⚙️ Variáveis de ambiente usadas pelo Dataflow Launcher
# ----------------------------------------------------------
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=30
ENV FLEX_TEMPLATES_NUM_LOG_LINES=1000

# ----------------------------------------------------------
# 🔍 Verificação opcional de estrutura
# ----------------------------------------------------------
RUN echo "🔍 Verificando estrutura do projeto..." && \
    find . -name "main_*.py" -type f && \
    ls -R ${WORKDIR}

# ----------------------------------------------------------
# 🚀 ENTRYPOINT PADRÃO
# ----------------------------------------------------------
ENTRYPOINT ["/opt/apache/beam/boot"]
