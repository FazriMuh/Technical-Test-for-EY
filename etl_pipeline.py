from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, sum as sum_col, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from prefect import task, flow
from prefect.server.schemas.schedules import RRuleSchedule
from prefect import serve
# Import NO_CACHE dari lokasi yang benar di Prefect terbaru
from prefect.tasks import NO_CACHE
import os
import warnings

warnings.filterwarnings('ignore')

# Setup SparkSession
current_dir = os.getcwd()
jdbc_jar_path = os.path.join(current_dir, "postgresql-42.7.4.jar")

spark = SparkSession.builder.appName("ETL Pipeline")\
    .config("spark.jars", jdbc_jar_path)\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Function to replace empty strings
def replace_empty_strings(df, columns_with_defaults):
    for col_name, default_value in columns_with_defaults.items():
        df = df.withColumn(col_name, 
                           when((col(col_name).isNull()) | (trim(col(col_name)) == ""), default_value)
                           .otherwise(col(col_name)))
    return df

@task
def extract():
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True)
    ])
    df = spark.read.option("multiline", "true").json("sample_data.json", schema=schema)
    return df

# Tambahkan cache_policy=NO_CACHE untuk mencegah Prefect mencoba cache DataFrame
@task(cache_policy=NO_CACHE)
def transform(df):
    columns_with_defaults = {
        "transaction_id": "unknown",
        "customer_id": "unknown",
        "currency": "USD",
        "status": "unknown"
    }
    df = replace_empty_strings(df, columns_with_defaults)

    df_with_successful_amount = df.withColumn(
        "successful_amount", 
        when(col("status") != "failed", col("amount")).otherwise(0.0)
    )

    customer_aggs = df_with_successful_amount.groupBy("customer_id").agg(
        count("transaction_id").alias("total_transactions"),
        sum_col("successful_amount").alias("total_amount")
    )
    customer_aggs.show()
    return customer_aggs

# Tambahkan cache_policy=NO_CACHE untuk mencegah Prefect mencoba cache DataFrame
@task(cache_policy=NO_CACHE)
def load(customer_aggs):
    pg_config = {
        "host": "localhost",
        "port": "5431",
        "database": "postgres",
        "user": "postgres",
        "password": "admin"
    }
    jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    try:
        customer_aggs.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "public.customer_aggs") \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print("Data berhasil disimpan ke PostgreSQL.")
    except Exception as e:
        print(f"Error saat menyimpan ke PostgreSQL: {e}")

@flow
def etl_pipeline():
    df = extract()
    customer_aggs = transform(df)
    load(customer_aggs)


if __name__ == "__main__":
    etl_pipeline()