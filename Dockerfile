FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

# 1. Rutas obligatorias para que Dataflow identifique los archivos
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

# 2. Configurar el directorio de trabajo y transferir el código
WORKDIR /template
COPY . /template

# 3. Actualizar herramientas básicas del sistema y de Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi-dev \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip

# 4. Instalar las dependencias controlando que no se rompa la imagen base
RUN pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# 5. Empaquetar dependencias para los Workers secundarios de GCP (Crucial)
RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE
