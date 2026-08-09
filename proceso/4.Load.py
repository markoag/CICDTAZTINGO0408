# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Load - Silver -> Golden
# MAGIC Genera las vistas de negocio (KPIs) listas para consumo en Power BI.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_proyecto_final")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

# MAGIC %md ## 4.1 KPI - Ventas Ecommerce por categoría

# COMMAND ----------

df_ecom = spark.table(f"{catalogo}.{esquema_source}.ecommerce_sales_cleaned")

df_kpi_ecommerce = df_ecom.groupBy(col("Product_Category")).agg(
    F.round(F.sum("Ingresos"), 2).alias("Ingresos_Totales"),
    F.sum("Units_Sold").cast(LongType()).alias("Unidades_Vendidas"),
    F.round(F.avg("Price"), 2).alias("Precio_Promedio"),
).orderBy(col("Ingresos_Totales").desc())

# COMMAND ----------

df_kpi_ecommerce.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.kpi_ventas_ecommerce")
print(f"✅ {catalogo}.{esquema_sink}.kpi_ventas_ecommerce: {df_kpi_ecommerce.count()} filas")

# COMMAND ----------

# MAGIC %md ## 4.2 KPI - Pedidos por Departamento / Pasillo (Aisle) en Instacart

# COMMAND ----------

df_instacart = spark.table(f"{catalogo}.{esquema_source}.instacart_orders_enriched")

df_kpi_instacart = df_instacart.groupBy(col("department"), col("aisle")).agg(
    F.count("product_id").cast(LongType()).alias("Total_Productos_Pedidos"),
    F.countDistinct("order_id").cast(LongType()).alias("Total_Ordenes"),
    F.countDistinct("product_id").cast(LongType()).alias("Productos_Distintos"),
).orderBy(col("Total_Productos_Pedidos").desc())

# COMMAND ----------

df_kpi_instacart.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.kpi_pedidos_por_departamento_aisle")
print(f"✅ {catalogo}.{esquema_sink}.kpi_pedidos_por_departamento_aisle: {df_kpi_instacart.count()} filas")

# COMMAND ----------

# MAGIC %md ## 4.3 KPI adicional - Tasa de recompra (reorder rate) por producto

# COMMAND ----------

df_kpi_reorder = df_instacart.groupBy(col("product_id"), col("product_name")).agg(
    F.count("*").cast(LongType()).alias("Veces_Pedido"),
    F.round(F.avg("reordered"), 3).alias("Tasa_Recompra"),
).filter(col("Veces_Pedido") >= 10) \
 .orderBy(col("Tasa_Recompra").desc())

# COMMAND ----------

df_kpi_reorder.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_sink}.kpi_tasa_recompra_producto")
print(f"✅ {catalogo}.{esquema_sink}.kpi_tasa_recompra_producto: {df_kpi_reorder.count()} filas")