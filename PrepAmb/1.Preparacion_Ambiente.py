# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Preparación de Ambiente - Proyecto Final
# MAGIC Reutiliza la misma infraestructura de tu ambiente de prueba (Storage
# MAGIC Account, Storage Credential `credential`, External Locations) pero crea
# MAGIC un **catálogo propio** (`catalog_proyecto_final`) y aísla sus datos en
# MAGIC subcarpetas propias (`bronze/proyecto_final`, `silver/proyecto_final`,
# MAGIC `golden/proyecto_final`) dentro de los mismos contenedores.
# MAGIC
# MAGIC ⚠️ A diferencia del notebook de prueba, este **NO** borra el catálogo
# MAGIC `catalog_au` ni el contenido completo de los contenedores bronze/silver/
# MAGIC golden — solo limpia su propia subcarpeta `proyecto_final/`, para no
# MAGIC afectar tu ambiente de prueba que ya funciona.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdata1702")
dbutils.widgets.text("catalogo", "catalog_proyecto_final")

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

print(f"storageName: {storageName}")
print(f"catalogo:    {catalogo}")

# COMMAND ----------

# MAGIC %md ## 1.1 External Locations (reutiliza la credencial `credential` ya existente)
# MAGIC `IF NOT EXISTS` -> si ya las creaste en el ambiente de prueba, no pasa nada.

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_metastore-adb`
URL 'abfss://metastore-adb@{storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para el metastore del Data Lake'
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_raw`
URL 'abfss://raw@{storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas raw del Data Lake'
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_bronze`
URL 'abfss://bronze@{storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake'
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_silver`
URL 'abfss://silver@{storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas silver del Data Lake'
""")

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_golden`
URL 'abfss://golden@{storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación externa para las tablas golden del Data Lake'
""")

print("✅ External locations verificadas/creadas")

# COMMAND ----------

# MAGIC %md ## 1.2 Catálogo propio del Proyecto Final (no toca `catalog_au`)

# COMMAND ----------

spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalogo}
MANAGED LOCATION 'abfss://metastore-adb@{storageName}.dfs.core.windows.net/proyecto_final'
COMMENT 'Catalogo para la arquitectura medallion del Proyecto Final (Ecommerce + Instacart)'
""")

# COMMAND ----------

# MAGIC %md ## 1.3 Esquemas de la Arquitectura Medallión

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalogo}.raw")
spark.sql(f"DROP SCHEMA IF EXISTS {catalogo}.bronze")
spark.sql(f"DROP SCHEMA IF EXISTS {catalogo}.silver")
spark.sql(f"DROP SCHEMA IF EXISTS {catalogo}.golden")

# COMMAND ----------

# MAGIC %md ## 1.4 Limpiar SOLO la subcarpeta "proyecto_final/" en cada contenedor
# MAGIC (no toca el resto del contenido de bronze/silver/golden, donde vive tu
# MAGIC ambiente de prueba `catalog_au`)

# COMMAND ----------

dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/", True)
dbutils.fs.rm(f"abfss://silver@{storageName}.dfs.core.windows.net/proyecto_final/", True)
dbutils.fs.rm(f"abfss://golden@{storageName}.dfs.core.windows.net/proyecto_final/", True)

print("🗑️  Subcarpetas proyecto_final/ limpiadas")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.raw")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.golden")

print(f"✅ Esquemas creados en {catalogo}")

# COMMAND ----------

# MAGIC %md ###Tablas Bronze

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.ecommerce_sales_raw (
  Date string,
  Product_Category string,
  Price double,
  Discount double,
  Customer_Segment string,
  Marketing_Spend double,
  Units_Sold integer,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/ecommerce_sales_raw'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.aisles (
  aisle_id integer,
  aisle string,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/aisles'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.departments (
  department_id integer,
  department string,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/departments'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.products (
  product_id integer,
  product_name string,
  aisle_id integer,
  department_id integer,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/products'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.orders (
  order_id integer,
  user_id integer,
  eval_set string,
  order_number integer,
  order_dow integer,
  order_hour_of_day integer,
  days_since_prior_order double,
  ingestion_date timestamp
)
USING DELTA
PARTITIONED BY (eval_set)
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/orders'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.order_products_prior (
  order_id integer,
  product_id integer,
  add_to_cart_order integer,
  reordered integer,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/order_products_prior'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.order_products_train (
  order_id integer,
  product_id integer,
  add_to_cart_order integer,
  reordered integer,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/order_products_train'
""")

print("✅ Tablas bronze creadas")

# COMMAND ----------

# MAGIC %md ###Tablas Silver

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.ecommerce_sales_cleaned (
  Order_Date date,
  Product_Category string,
  Price double,
  Discount double,
  Customer_Segment string,
  Marketing_Spend double,
  Units_Sold integer,
  Ingresos double,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://silver@{storageName}.dfs.core.windows.net/proyecto_final/ecommerce_sales_cleaned'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.instacart_orders_enriched (
  order_id integer,
  product_id integer,
  product_name string,
  aisle string,
  department string,
  user_id integer,
  order_number integer,
  order_dow integer,
  order_hour_of_day integer,
  reordered integer
)
USING DELTA
LOCATION 'abfss://silver@{storageName}.dfs.core.windows.net/proyecto_final/instacart_orders_enriched'
""")

print("✅ Tablas silver creadas")

# COMMAND ----------

# MAGIC %md ###Tablas Golden

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.kpi_ventas_ecommerce (
  Product_Category string,
  Ingresos_Totales double,
  Unidades_Vendidas long,
  Precio_Promedio double
)
USING DELTA
LOCATION 'abfss://golden@{storageName}.dfs.core.windows.net/proyecto_final/kpi_ventas_ecommerce'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.kpi_pedidos_por_departamento_aisle (
  department string,
  aisle string,
  Total_Productos_Pedidos long,
  Total_Ordenes long,
  Productos_Distintos long
)
USING DELTA
LOCATION 'abfss://golden@{storageName}.dfs.core.windows.net/proyecto_final/kpi_pedidos_por_departamento_aisle'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.kpi_tasa_recompra_producto (
  product_id integer,
  product_name string,
  Veces_Pedido long,
  Tasa_Recompra double
)
USING DELTA
LOCATION 'abfss://golden@{storageName}.dfs.core.windows.net/proyecto_final/kpi_tasa_recompra_producto'
""")

print("✅ Tablas golden creadas")
print("🎉 Ambiente del Proyecto Final preparado correctamente.")
