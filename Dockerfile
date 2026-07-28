# 1. Imagen base oficial de Google Cloud optimizada con Python 3.11
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# 2. Variables de entorno obligatorias para el launcher moderno de Dataflow (Usando MAIN_FILE)
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE="/template/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

# 3. ORDEN RECOMENDADO: Primero definimos y creamos la carpeta de trabajo interna
WORKDIR /template

# 4. Copiamos TODO el contenido de tu GitHub dentro de la carpeta /template de forma masiva
COPY . /template

# 5. Actualizamos pip e instalamos las dependencias directo en el contenedor launcher
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# 6. Evita descargas redundantes de dependencias en la red interna de Dataflow
ENV PIP_NO_DEPS=1


