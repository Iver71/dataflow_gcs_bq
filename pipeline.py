import argparse
import logging
import csv
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

    def setup(self):
        self.client = bigquery.Client(project=self.project_id)

    def process(self, element):
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
        query_job = self.client.query(query_transformacion, location=self.location)
        query_job.result()  
        logging.info("Capa SILVER completada exitosamente desde Dataflow.")
        yield f"Proceso Silver Exitoso para {self.table_id}"

def parse_safe_csv(line):
    """Parsea el texto delimitado manejando comas internas de los textos"""
    reader = csv.reader([line], delimiter=',', quotechar='"')
    for row in reader:
        if len(row) >= 7:
            return dict(zip(
                ['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'],
                [val.strip() for val in row[:7]]
            ))
    return None

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_uri", default="gs://tables_sample/olist_order_reviews_dataset.csv")
    parser.add_argument("--project_id", default="omega-chimera-469104-s7")
    parser.add_argument("--location", default="us-east1")
    parser.add_argument("--table_id", default="order_reviews")
    parser.add_argument("--dataset_bronze", default="olist_dataset_bronze")
    parser.add_argument("--dataset_silver", default="olist_dataset_silver")

    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project_id,
        region=known_args.location
    )

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
        # PASO 1: Ingesta desde Cloud Storage y almacenamiento en capa Bronze
        bronze_outputs = (
            p
            | "Read CSV from GCS" >> beam.io.ReadFromText(known_args.input_uri, skip_header_lines=1)
            | "Parse CSV Line" >> beam.Map(parse_safe_csv)
            | "Filter Invalid Lines" >> beam.Filter(lambda x: x is not None)
            | "Write to Bronze Table" >> beam.io.WriteToBigQuery(
                table=known_args.table_id,
                dataset=known_args.dataset_bronze,
                project=known_args.project_id,
                schema=esquema_bronze,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

        # CORRECCIÓN DEFINITIVA: Extraer la PCollection válida de control de BigQuery
        bronze_signal = bronze_outputs.destination_load_job_ids_pc

        # PASO 2: Orquestación controlada de la ejecución de capa Silver
        (
            p
            | "Create Trigger Signal" >> beam.Create([None])
            | "Wait for Bronze Load" >> beam.Map(
                lambda x, signal: x, 
                signal=beam.pvalue.AsSingleton(bronze_signal)
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

