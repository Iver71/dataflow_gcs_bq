# 1. Usar la imagen base oficial de Google Cloud para Flex Templates con Python 3.11
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# 2. Definir el directorio de trabajo dentro del contenedor
WORKDIR /dataflow/template

# 3. Copiar los archivos de tu repositorio hacia el contenedor
COPY pipeline.py /dataflow/template/pipeline.py
COPY requirements.txt /dataflow/template/requirements.txt

# 4. DEFINIR LAS VARIABLES DE ENTORNO OBLIGATORIAS
# Esto le dice al launcher de Dataflow exactamente qué archivo ejecutar
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/dataflow/template/requirements.txt"

# 5. Actualizar pip e instalar las dependencias necesarias
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /dataflow/template/requirements.txt

# Nota: No se necesita definir un ENTRYPOINT, la imagen base se encarga de eso automáticamente

