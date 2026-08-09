# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta - Dataset Instacart Market Basket
# MAGIC Lee los 6 CSV de negocio del dataset de Instacart (separados por `;`)
# MAGIC desde la capa `raw` y los inserta en sus tablas Delta pre-creadas en
# MAGIC `bronze`.
# MAGIC
# MAGIC Nota: `sample_submission.csv` **no** se ingesta aquí porque es un
# MAGIC archivo de referencia del concurso de Kaggle (no es un dataset de
# MAGIC negocio) — solo se conserva en `raw` como respaldo.
# MAGIC
# MAGIC Nota 2: `products.csv` tiene un `;` final en cada línea que genera una
# MAGIC 5ta columna vacía en el archivo original — se descarta al leer.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_proyecto_final")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("storageName", "adstaztingo0408prod")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
storageName = dbutils.widgets.get("storageName")

base_path = f"abfss://{container}@{storageName}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md ## 2.1 aisles.csv

# COMMAND ----------

aisles_schema = StructType(fields=[
    StructField("aisle_id", IntegerType(), False),
    StructField("aisle", StringType(), True),
])

df_aisles = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(aisles_schema) \
    .csv(f"{base_path}/aisles.csv")

aisles_final_df = df_aisles.withColumn("ingestion_date", current_timestamp())
aisles_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.aisles")
print(f"✅ {catalogo}.{esquema}.aisles: {aisles_final_df.count()} filas")

# COMMAND ----------

# MAGIC %md ## 2.2 departments.csv

# COMMAND ----------

departments_schema = StructType(fields=[
    StructField("department_id", IntegerType(), False),
    StructField("department", StringType(), True),
])

df_departments = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(departments_schema) \
    .csv(f"{base_path}/departments.csv")

departments_final_df = df_departments.withColumn("ingestion_date", current_timestamp())
departments_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.departments")
print(f"✅ {catalogo}.{esquema}.departments: {departments_final_df.count()} filas")

# COMMAND ----------

# MAGIC %md ## 2.3 products.csv (descarta la 5ta columna vacía por el `;` final)

# COMMAND ----------

products_schema = StructType(fields=[
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("_extra_col", StringType(), True),  # columna fantasma por el ";" final
])

df_products = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(products_schema) \
    .csv(f"{base_path}/products.csv")

products_final_df = df_products \
    .drop("_extra_col") \
    .withColumn("ingestion_date", current_timestamp())

products_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.products")
print(f"✅ {catalogo}.{esquema}.products: {products_final_df.count()} filas")

# COMMAND ----------

# MAGIC %md ## 2.4 orders.csv (days_since_prior_order puede venir vacío en el primer pedido)

# COMMAND ----------

orders_schema = StructType(fields=[
    StructField("order_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", IntegerType(), True),
    StructField("order_hour_of_day", IntegerType(), True),
    StructField("days_since_prior_order", DoubleType(), True),
])

df_orders = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(orders_schema) \
    .csv(f"{base_path}/orders.csv")

orders_final_df = df_orders.withColumn("ingestion_date", current_timestamp())
orders_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.orders")
print(f"✅ {catalogo}.{esquema}.orders: {orders_final_df.count()} filas")

# COMMAND ----------

# MAGIC %md ## 2.5 order_products__prior.csv

# COMMAND ----------

order_products_schema = StructType(fields=[
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True),
])

df_op_prior = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(order_products_schema) \
    .csv(f"{base_path}/order_products__prior.csv")

op_prior_final_df = df_op_prior.withColumn("ingestion_date", current_timestamp())
op_prior_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.order_products_prior")
print(f"✅ {catalogo}.{esquema}.order_products_prior: {op_prior_final_df.count()} filas")

# COMMAND ----------

# MAGIC %md ## 2.6 order_products__train.csv

# COMMAND ----------

df_op_train = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(order_products_schema) \
    .csv(f"{base_path}/order_products__train.csv")

op_train_final_df = df_op_train.withColumn("ingestion_date", current_timestamp())
op_train_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.order_products_train")
print(f"✅ {catalogo}.{esquema}.order_products_train: {op_train_final_df.count()} filas")