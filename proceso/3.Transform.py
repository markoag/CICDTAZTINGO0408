# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Transform - Bronze -> Silver
# MAGIC 1. Limpia, castea y calcula ingresos de `ecommerce_sales_raw`.
# MAGIC 2. Une las tablas de Instacart (`order_products_prior` + `orders` +
# MAGIC    `products` + `aisles` + `departments`) en una tabla enriquecida.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_proyecto_final")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

# MAGIC %md ## 3.1 Limpieza - Ecommerce Sales

# COMMAND ----------

df_ecom = spark.table(f"{catalogo}.{esquema_source}.ecommerce_sales_raw")

df_ecom = df_ecom.dropna(how="all") \
                  .filter((col("Price").isNotNull()) & (col("Units_Sold").isNotNull()))

# COMMAND ----------

df_ecom_clean = df_ecom \
    .withColumn("Order_Date", to_date(col("Date"), "dd-MM-yyyy")) \
    .withColumn("Product_Category", trim(col("Product_Category"))) \
    .withColumn("Customer_Segment", trim(col("Customer_Segment"))) \
    .withColumn("Price", col("Price").cast(DoubleType())) \
    .withColumn("Discount", col("Discount").cast(DoubleType())) \
    .withColumn("Marketing_Spend", col("Marketing_Spend").cast(DoubleType())) \
    .withColumn("Units_Sold", col("Units_Sold").cast(IntegerType())) \
    .withColumn(
        "Ingresos",
        F.round(col("Price") * (1 - col("Discount") / 100) * col("Units_Sold"), 2)
    ) \
    .dropDuplicates() \
    .select(
        "Order_Date", "Product_Category", "Price", "Discount",
        "Customer_Segment", "Marketing_Spend", "Units_Sold",
        "Ingresos", "ingestion_date"
    )

# COMMAND ----------

df_ecom_clean.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.ecommerce_sales_cleaned")
print(f"✅ {catalogo}.{esquema_sink}.ecommerce_sales_cleaned: {df_ecom_clean.count()} filas")

# COMMAND ----------

# MAGIC %md ## 3.2 Enriquecimiento - Instacart
# MAGIC Une `order_products_prior` con `orders`, `products`, `aisles` y
# MAGIC `departments` para tener una tabla analítica plana.

# COMMAND ----------

df_order_products = spark.table(f"{catalogo}.{esquema_source}.order_products_prior") \
                          .dropna(how="all") \
                          .dropDuplicates(["order_id", "product_id"])

df_orders = spark.table(f"{catalogo}.{esquema_source}.orders") \
                  .dropna(how="all") \
                  .filter(col("order_id").isNotNull())

df_products = spark.table(f"{catalogo}.{esquema_source}.products") \
                    .filter(col("product_id").isNotNull())

df_aisles = spark.table(f"{catalogo}.{esquema_source}.aisles")
df_departments = spark.table(f"{catalogo}.{esquema_source}.departments")

# COMMAND ----------

df_instacart_joined = df_order_products.alias("op") \
    .join(df_orders.alias("o"), col("op.order_id") == col("o.order_id"), "inner") \
    .join(df_products.alias("p"), col("op.product_id") == col("p.product_id"), "left") \
    .join(df_aisles.alias("a"), col("p.aisle_id") == col("a.aisle_id"), "left") \
    .join(df_departments.alias("d"), col("p.department_id") == col("d.department_id"), "left")

# COMMAND ----------

df_instacart_enriched = df_instacart_joined.select(
    col("op.order_id").alias("order_id"),
    col("op.product_id").alias("product_id"),
    col("p.product_name").alias("product_name"),
    col("a.aisle").alias("aisle"),
    col("d.department").alias("department"),
    col("o.user_id").alias("user_id"),
    col("o.order_number").alias("order_number"),
    col("o.order_dow").alias("order_dow"),
    col("o.order_hour_of_day").alias("order_hour_of_day"),
    col("op.reordered").alias("reordered"),
)

# COMMAND ----------

df_instacart_enriched.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.instacart_orders_enriched")
print(f"✅ {catalogo}.{esquema_sink}.instacart_orders_enriched: {df_instacart_enriched.count()} filas")