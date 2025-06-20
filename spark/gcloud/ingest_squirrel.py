from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def save_parquet(df, path):
    df.write.format("parquet").mode("overwrite").save(path)
  
def create_spark_session():
    spark = SparkSession.builder.master('local').appName('Batch API Ingestion').getOrCreate()
    return spark

def read_csv(spark: SparkSession, path: str):    
    df = spark.read.format("csv").load(path, sep=",", header=True)
    return df

if __name__ == '__main__':

    # add this .py into the bucket (gs://edit-data-eng-dev/datalake/scripts/ingest_squirrel.py) 
    # run: gcloud dataproc batches submit pyspark gs://edit-data-eng-dev/datalake/scripts/ingest_squirrel.py --batch ingestsquirrel01 --deps-bucket=edit-data-eng-dev --region=europe-west1

    spark = create_spark_session()

    # ingest squirrel
    squirrel_path = "gs://edit-data-eng-dev/datalake/landing/squirrel/squirrel"
    df = read_csv(spark, squirrel_path)
    df.head(10)
    save_parquet(df, "gs://edit-data-eng-dev/datalake/bronze/squirrel/")
