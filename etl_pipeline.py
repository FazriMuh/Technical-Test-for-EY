from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, sum as sum_col, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from prefect import task, flow
from prefect.server.schemas.schedules import RRuleSchedule
from prefect import serve
from prefect.tasks import NO_CACHE
import os
import warnings

warnings.filterwarnings('ignore')  # Ignore warnings to keep the output clean

# Setup SparkSession
current_dir = os.getcwd()
jdbc_jar_path = os.path.join(current_dir, "postgresql-42.7.4.jar")

spark = SparkSession.builder.appName("ETL Pipeline")\
    .config("spark.jars", jdbc_jar_path)\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Reduce Spark log verbosity

# Function to replace empty strings with default values
def replace_empty_strings(df, columns_with_defaults):
    for col_name, default_value in columns_with_defaults.items():
        df = df.withColumn(col_name, 
                           when((col(col_name).isNull()) | (trim(col(col_name)) == ""), default_value)
                           .otherwise(col(col_name)))
    return df

@task
def extract():
    # Define schema for reading JSON file
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True)
    ])
    # Read JSON file into a Spark DataFrame
    df = spark.read.option("multiline", "true").json("sample_data.json", schema=schema)
    return df

@task(cache_policy=NO_CACHE)
def transform(df):
    # Replace empty string values with defaults
    columns_with_defaults = {
        "transaction_id": "unknown",
        "customer_id": "unknown",
        "currency": "USD",
        "status": "unknown"
    }
    df = replace_empty_strings(df, columns_with_defaults)

    # Create new column for successful transaction amounts
    df_with_successful_amount = df.withColumn(
        "successful_amount", 
        when(col("status") != "failed", col("amount")).otherwise(0.0)
    )

    # Aggregate data per customer
    customer_aggs = df_with_successful_amount.groupBy("customer_id").agg(
        count("transaction_id").alias("total_transactions"),
        sum_col("successful_amount").alias("total_amount")
    )
    customer_aggs.show()  # Display results
    return customer_aggs

@task(cache_policy=NO_CACHE)
def load(customer_aggs):
    # Define PostgreSQL connection parameters
    pg_config = {
        "host": "localhost",
        "port": "5431",
        "database": "postgres",
        "user": "postgres",
        "password": "admin"
    }
    jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    
    # Write DataFrame to PostgreSQL database
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
        print("Data successfully saved to PostgreSQL.")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")

@flow
def etl_pipeline():
    df = extract()  # Extract data from JSON
    customer_aggs = transform(df)  # Transform data
    load(customer_aggs)  # Load data into PostgreSQL

if __name__ == "__main__":
    etl_pipeline()  # Run the ETL pipeline
