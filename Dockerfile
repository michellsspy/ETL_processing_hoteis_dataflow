# ==========================================================
# 🧩 BASE IMAGE OFICIAL — Dataflow Python 3.12 + Beam 2.64.0
# ==========================================================
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:beam-2.64.0

# ----------------------------------------------------------
# 📁 Diretório de trabalho padrão dentro do container
# ----------------------------------------------------------
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# ----------------------------------------------------------
# 📦 Copiar apenas o requirements.txt primeiro (para cache)
# ----------------------------------------------------------
COPY requirements.txt .

# Atualiza o pip e instala as dependências antes do código
RUN pip install --upgrade pip && \
    pip install -U -r requirements.txt

# ----------------------------------------------------------
# 📂 Agora copia todo o código fonte do projeto
# ----------------------------------------------------------
COPY . .

# ----------------------------------------------------------
# ⚙️ Variáveis de ambiente usadas pelo Template Launcher
# ----------------------------------------------------------
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=30
ENV FLEX_TEMPLATES_NUM_LOG_LINES=1000

# ----------------------------------------------------------
# 🔍 Etapa opcional de verificação de estrutura
# ----------------------------------------------------------
RUN echo "🔍 Verificando estrutura do projeto..." && \
    echo "📁 Arquivos principais encontrados:" && \
    find . -name "main_*.py" -type f && \
    echo "📁 Estrutura do diretório atual:" && \
    ls -R ${WORKDIR}

# ----------------------------------------------------------
# 🚀 ENTRYPOINT PADRÃO — nunca altere para Dataflow
# ----------------------------------------------------------
ENTRYPOINT ["/opt/apache/beam/boot"]
