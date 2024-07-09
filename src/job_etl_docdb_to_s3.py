import sys
import logging
import pyspark
import json
from pyspark.sql.functions import col, year, month, dayofmonth, lit
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from datetime import datetime, timedelta, timezone

# Configura logging
logger = logging.getLogger("pcm_etl_docdb_to_s3")
logger.setLevel(logging.INFO)

# Obtém parâmetros do job
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'BUCKET_NAME', 'DATABASE_NAME', 'MONGO_URI', 'OUTPUT_PREFIX', 'ENVIRONMENT_ID',
     'DB_CONNECTION_NAME', 'CUSTOMERS', 'REPORT_TYPES']
)


def validate_arguments(required_params):
    required_params = ['JOB_NAME', 'BUCKET_NAME', 'DATABASE_NAME', 'MONGO_URI', 'OUTPUT_PREFIX',
                       'ENVIRONMENT_ID', 'CUSTOMERS', 'REPORT_TYPES', 'DB_CONNECTION_NAME']

    for param in required_params:
        if param not in args or not args[param].strip():
            raise ValueError(f"The parameter '{param}' is missing or empty.")


validate_arguments(args)

job_name = args['JOB_NAME']
bucket_name = args['BUCKET_NAME']
database_name = args['DATABASE_NAME']
mongo_uri = args['MONGO_URI']
output_prefix = args['OUTPUT_PREFIX']
environment_id = args['ENVIRONMENT_ID']
customers = args['CUSTOMERS'].split(",")
report_types = args['REPORT_TYPES'].split(",")
db_connection_name = args['DB_CONNECTION_NAME']


def get_collection_name(customer_id, collection_type):
    return f"{environment_id}.{customer_id}.{collection_type}.reports"


def define_query_dates():
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    gte = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, tzinfo=timezone.utc)
    lte = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=timezone.utc)

    return gte.isoformat(), lte.isoformat()


def write_data(df):
    s3_output_path = f"s3://{bucket_name}/{output_prefix}"
    logger.info(f"Writing data to S3 path at {s3_output_path}")
    (df.write
     .partitionBy("customer_id", "report_type", "year", "month", "day")
     .format("parquet")
     .mode("append")
     .option("path", s3_output_path)
     .save())
    logger.info(f"Data written to S3 path at {s3_output_path}")


def read_data(spark, collection_name, report_type, customer_id):
    # Define as datas de início e fim da query
    yesterday_start, yesterday_end = define_query_dates()

    # Configuração para ler do DocumentDB
    pipeline = [
        {
            "$match": {
                "timestamp": {
                    "$gte": {"$date": yesterday_start},
                    "$lte": {"$date": yesterday_end}
                }
            }
        }
    ]

    df = (spark.read.format("mongodb")
          .option("aggregation.pipeline", json.dumps(pipeline))
          .option("database", database_name)
          .option("connection.uri", mongo_uri)
          .option("collection", collection_name)
          .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner")
          .load())

    if df.isEmpty():
        logger.info(f"No data found in collection {collection_name}")
        return None

    # Adiciona colunas de ano, mês, dia, report_type e customer_id
    df = df.withColumn("year", year(col("timestamp")))
    df = df.withColumn("month", month(col("timestamp")))
    df = df.withColumn("day", dayofmonth(col("timestamp")))
    df = df.withColumn("customer_id", lit(customer_id))
    df = df.withColumn("report_type", lit(report_type))

    return df


def main():
    # Inicializa o Glue e Spark context
    sc = pyspark.SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)

    job.init(job_name, args)

    for customer_id in customers:
        logger.debug(f"Processing for customer_id [{customer_id}]")
        for report_type in report_types:
            logger.debug(f"Processing for report_type [{report_type}]")
            collection_name = get_collection_name(customer_id, report_type)

            logger.info(f"Processing for collection {collection_name}")
            # Lê os dados do DocumentDB
            df = read_data(spark, collection_name, report_type, customer_id)

            if df is None:
                logger.info(f"No data found in collection {collection_name}")
            else:
                # Escreve os dados no S3
                write_data(df)

    job.commit()


if __name__ == "__main__":
    main()
