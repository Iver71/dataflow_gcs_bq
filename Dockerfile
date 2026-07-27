# 1. Usamos la imagen base oficial requerida para Python 3.11 y Beam 2.60.0
FROM apache/beam_python3.11_sdk:2.60.0

# 2. Definimos el directorio de trabajo estándar para Flex Templates
WORKDIR /template

# 3. Copiamos los requisitos primero para optimizar el tiempo de compilación (Caché de Docker)
COPY requirements.txt .

# 4. Instalamos las dependencias necesarias sin guardar caché para reducir el tamaño del contenedor
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Copiamos el script del pipeline y el archivo de metadatos obligatorio
COPY pipeline.py .
COPY metadata.json .

# 6. Definimos las variables de entorno para que el SDK de Beam localice el script principal
ENV FLEX_TEMPLATE_PYTHON_MAIN_FILE="/template/pipeline.py"

# 7. Punto de entrada obligatorio del SDK de Apache Beam para arrancar los workers de Dataflow
ENTRYPOINT ["/opt/apache/beam/boot"]
