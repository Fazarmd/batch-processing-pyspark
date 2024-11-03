import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp,col,when
from dotenv import load_dotenv

dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster(spark_host)
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# define schema
costumers_schema = "customer_id string, customer_unique_id string, customer_zip_code_prefix string, customer_city string, customer_state string"

orders_schema = "order_id string, customer_id string, order_status string, order_purchase_timestamp string, order_approved_at string, order_delivered_carrier_date string, order_delivered_customer_date string, order_estimated_delivery_date string"

# read csv
customers_df = spark.read.csv("/data/olist/olist_customers_dataset.csv", header=True, schema=costumers_schema)
orders_df = spark.read.csv("/data/olist/olist_orders_dataset.csv", header=True, schema=orders_schema)

# write to postgres
customers_df.write.mode("overwrite").jdbc(
    jdbc_url,
    'public.olist_customers',
    properties=jdbc_properties
)

orders_df.write.mode("overwrite").jdbc(
    jdbc_url,
    'public.olist_orders',
    properties=jdbc_properties
)

# read back from postgres
customers_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_customers',
    properties=jdbc_properties
)

orders_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_orders',
    properties=jdbc_properties
)

# create temporary views
customers_df.createOrReplaceTempView("olist_customers")
orders_df.createOrReplaceTempView("olist_orders")

# analysis jumlah orderan dan costumer berdasarkan state
customer_orders = spark.sql("""
    SELECT 
        c.customer_state,
        COUNT(DISTINCT o.order_id) as total_orders,
        COUNT(DISTINCT c.customer_id) as total_customers,
        ROUND(COUNT(DISTINCT o.order_id) * 1.0 / COUNT(DISTINCT c.customer_id), 2) as orders_per_customer
    FROM olist_customers c
    LEFT JOIN olist_orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_state
    ORDER BY orders_per_customer DESC
""")

# write analysis results
customer_orders.write.mode("overwrite").jdbc(
    jdbc_url,
    'public.olist_customer_order_analysis',
    properties=jdbc_properties
)

# Show results
print("\n=== Customer Order Analysis by State ===")
customer_orders.show(5)