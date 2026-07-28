import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

class ExecuteSilverTransformFn(beam.DoFn):
    def __init__(self, project_id, dataset_bronze, dataset_silver, table_id, location):
        self.project_id = project_id
        self.dataset_bronze = dataset_bronze
        self.dataset_silver = dataset_silver
        self.table_id = table_id
        self.location = location

    def process(self, element):
        client = bigquery.Client(project=self.project_id)
        
        table_ref_silver = f"`{self.project_id}.{self.dataset_silver}.{self.table_id}`"
        table_source_bronze = f"`{self.project_id}.{self.dataset_bronze}.{self.table_id}`"
        
        query_transformacion = f"""
        CREATE OR REPLACE TABLE {table_ref_silver} AS
        SELECT
            CAST(review_id AS STRING) AS review_id,
            CAST(order_id AS STRING) AS order_id,
            CAST(review_score AS INT64) AS review_score,
            CAST(review_comment_title AS STRING) AS review_comment_title,
            CAST(review_comment_message AS STRING) AS review_comment_message,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', review_creation_date) AS review_creation_date,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', review_answer_timestamp) AS review_answer_timestamp
        FROM
            {table_source_bronze}
        """
        
        logging.info("Iniciando transformación hacia la capa SILVER en BigQuery...")
        query_job = client.query(query_transformacion, location=self.location)
        query_job.result()  
        logging.info("Capa SILVER completada exitosamente desde Dataflow.")
        yield f"Proceso Silver Exitoso para {self.table_id}"

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_uri", default="gs://tables_sample/olist_order_reviews_dataset.csv")
    parser.add_argument("--project_id", default="omega-chimera-469104-s7")
    parser.add_argument("--location", default="us-east1")
    parser.add_argument("--table_id", default="order_reviews")
    parser.add_argument("--dataset_bronze", default="olist_dataset_bronze")
    parser.add_argument("--dataset_silver", default="olist_dataset_silver")

    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Configuramos las opciones de Beam con el proyecto y la región
    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project_id,
        region=known_args.location
    )

    # ----------------------------------------------------------------------
    # Verificación de Datasets para asegurar la existencia de las tablas
    # ----------------------------------------------------------------------
    try:
        bq_client = bigquery.Client(project=known_args.project_id)
        datasets_to_check = [known_args.dataset_bronze, known_args.dataset_silver]
        
        for dataset_name in datasets_to_check:
            dataset_ref = bigquery.DatasetReference(known_args.project_id, dataset_name)
            try:
                bq_client.get_dataset(dataset_ref)
                logging.info(f"El dataset {dataset_name} ya existe.")
            except Exception:
                logging.info(f"El dataset {dataset_name} no existe. Creándolo...")
                nuevo_dataset = bigquery.Dataset(dataset_ref)
                nuevo_dataset.location = known_args.location
                bq_client.create_dataset(nuevo_dataset, exists_ok=True)
                logging.info(f"Dataset {dataset_name} creado con éxito en {known_args.location}.")
    except Exception as e:
        logging.warning(f"Validación preliminar de datasets omitida: {e}")

    # Esquema para la Capa Bronze
    esquema_bronze = {
        'fields': [
            {'name': 'review_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_score', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_comment_title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_comment_message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_creation_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_answer_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        # PASO 1: Ingesta desde Cloud Storage y Escritura a Capa Bronze
        bronze_load = (
            p
            | "Read CSV from GCS" >> beam.io.ReadFromText(known_args.input_uri, skip_header_lines=1)
            | "Parse CSV Line" >> beam.Map(lambda line: dict(zip(
                ['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'],
                [val.strip('"') for val in line.split(',')]
              )))
            | "Write to Bronze Table" >> beam.io.WriteToBigQuery(
                table=known_args.table_id,
                dataset=known_args.dataset_bronze,
                project=known_args.project_id,
                schema=esquema_bronze,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

        # PASO 2: Transformación SQL hacia Capa Silver
        (
            p
            | "Create Trigger Signal" >> beam.Create([None])
            | "Wait for Bronze Load" >> beam.Map(
                lambda x, onyx: x, 
                onyx=beam.pvalue.AsSingleton(bronze_load.destination_load_jobid_pcollection)
            )
            | "Execute SQL Silver" >> beam.ParDo(
                ExecuteSilverTransformFn(
                    project_id=known_args.project_id,
                    dataset_bronze=known_args.dataset_bronze,
                    dataset_silver=known_args.dataset_silver,
                    table_id=known_args.table_id,
                    location=known_args.location
                )
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
