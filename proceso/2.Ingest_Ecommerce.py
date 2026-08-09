# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta - Ecommerce Sales Prediction Dataset
# MAGIC Lee `Ecommerce_Sales_Prediction_Dataset.csv` (separado por coma) desde
# MAGIC la capa `raw` y lo inserta en la tabla Delta pre-creada en `bronze`.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_proyecto_final")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("storageName", "adstaztingo0408")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
storageName = dbutils.widgets.get("storageName")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/Ecommerce_Sales_Prediction_Dataset.csv"

# COMMAND ----------

# El CSV real usa coma como separador y las columnas ya vienen con estos
# nombres exactos: Date,Product_Category,Price,Discount,Customer_Segment,
# Marketing_Spend,Units_Sold
ecommerce_schema = StructType(fields=[
    StructField("Date", StringType(), True),
    StructField("Product_Category", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Customer_Segment", StringType(), True),
    StructField("Marketing_Spend", DoubleType(), True),
    StructField("Units_Sold", IntegerType(), True),
])

# COMMAND ----------

df_ecommerce = spark.read \
    .option("header", True) \
    .option("sep", ",") \
    .schema(ecommerce_schema) \
    .csv(ruta)

# COMMAND ----------

ecommerce_final_df = df_ecommerce.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

ecommerce_final_df.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.ecommerce_sales_raw")

print(f"✅ {catalogo}.{esquema}.ecommerce_sales_raw: {ecommerce_final_df.count()} filas")