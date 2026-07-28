# 1. Imagen base
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# 2. Definición de variables globales (No importa el orden entre ellas)
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE="/template/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

# 3. ORDEN ESTRICTO: Primero creamos/entramos al directorio...
WORKDIR /template

# 4. ...y después copiamos los archivos dentro de él
COPY . /template

# 5. Instalación de dependencias
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

ENV PIP_NO_DEPS=1
