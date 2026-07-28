# 1. Usamos la imagen base oficial de Google Cloud para Flex Templates con Python 3.11
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# 2. Definimos el directorio de trabajo estándar para Flex Templates
WORKDIR /template

# 3. Copiamos los requisitos primero para optimizar el tiempo de compilación
COPY requirements.txt .

# 4. Instalamos las dependencias necesarias sin guardar caché
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Copiamos el script del pipeline y el archivo de metadatos obligatorio
COPY pipeline.py .
COPY metadata.json .

# 6. ¡CORREGIDO!: Sin comillas dobles para evitar fallos de lectura de entorno
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE=/template/pipeline.py

# 7. Punto de entrada obligatorio para Dataflow Flex Templates
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
