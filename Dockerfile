# 1. Usamos una versión base moderna con Python 3.11 para evitar advertencias de obsolescencia
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# 2. Definimos las rutas obligatorias para la Flex Template dentro del contenedor
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE="/template/pipeline.py"

# 3. Copiamos todos los archivos del repositorio al directorio de trabajo
COPY . /template
WORKDIR /template

# 4. Instalamos las dependencias en la imagen base (necesario para el paso de inicialización)
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# 5. Le indicamos a Apache Beam que descargue las librerías en los Workers usando el Administrador de Paquetes de Dataflow
ENV PIP_NO_DEPS=1
