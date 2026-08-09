# Databricks notebook source
# MAGIC %md
# MAGIC # Rollback / Reversión del Ambiente - Proyecto Final Medallion
# MAGIC Deshace, en orden inverso, todo lo creado por:
# MAGIC - `proceso/1.Preparacion_Ambiente.py`
# MAGIC - `proceso/2.Ingest_Ecommerce.py`, `proceso/2.Ingest_Instacart.py`
# MAGIC - `proceso/3.Transform.py`
# MAGIC - `proceso/4.Load.py`
# MAGIC - `proceso/5.Grants_Medallion.py`
# MAGIC
# MAGIC Ejecutar manualmente cuando necesites limpiar el ambiente por completo
# MAGIC (por ejemplo, para volver a correr el pipeline desde cero o para
# MAGIC desmontar el proyecto).
# MAGIC
# MAGIC ⚠️ **ESTE NOTEBOOK ES DESTRUCTIVO** para el catálogo `catalog_proyecto_final`
# MAGIC y su subcarpeta física `proyecto_final/`. **NO** borra external locations,
# MAGIC la storage credential `credential`, ni el catálogo `catalog_au` de tu
# MAGIC ambiente de prueba (son infraestructura compartida).

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdata1702")
dbutils.widgets.text("catalogo", "catalog_proyecto_final")

storageName = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

print(f"storageName a limpiar: {storageName}")
print(f"Catálogo a revertir:   {catalogo}")
print("⚠️  Este rollback SOLO afecta 'catalog_proyecto_final' y la subcarpeta")
print("    'proyecto_final/' en bronze/silver/golden. No toca 'catalog_au'.")

# COMMAND ----------

# MAGIC %md ## 1. Revocar privilegios (RBAC) otorgados en 5.Grants_Medallion

# COMMAND ----------

principals_bronze_silver = ["`analista1@hotmail.com`", "`Devops`"]

for p in principals_bronze_silver:
    spark.sql(f"REVOKE USE SCHEMA ON SCHEMA `{catalogo}`.bronze FROM {p}")
    spark.sql(f"REVOKE USE SCHEMA ON SCHEMA `{catalogo}`.silver FROM {p}")

spark.sql(f"REVOKE CREATE TABLE ON SCHEMA `{catalogo}`.bronze FROM `analista1@hotmail.com`")
spark.sql(f"REVOKE USE CATALOG ON CATALOG `{catalogo}` FROM `analista1@hotmail.com`")

spark.sql(f"REVOKE SELECT ON SCHEMA `{catalogo}`.golden FROM `DAs`")
spark.sql(f"REVOKE USE SCHEMA ON SCHEMA `{catalogo}`.golden FROM `DAs`")
spark.sql(f"REVOKE USE CATALOG ON CATALOG `{catalogo}` FROM `DAs`")

print("✅ Privilegios revocados (analista1@hotmail.com, Devops, DAs)")

# COMMAND ----------

# MAGIC %md ## 2. Eliminar tablas de la capa GOLDEN (creadas en 4.Load.py)

# COMMAND ----------

tablas_golden = [
    "kpi_ventas_ecommerce",
    "kpi_pedidos_por_departamento_aisle",
    "kpi_tasa_recompra_producto",
]
# ⚠️ NOTA: los notebooks de prueba usan `insertInto` sobre tablas Delta
# pre-creadas con LOCATION propio. Al hacer DROP TABLE, Databricks borra el
# metadato en Unity Catalog pero NO borra automáticamente los archivos Delta
# físicos (porque están en una LOCATION externa explícita, no managed). Por
# eso el paso 6 de este notebook limpia también la carpeta física.

for tabla in tablas_golden:
    spark.sql(f"DROP TABLE IF EXISTS `{catalogo}`.golden.{tabla}")
    print(f"🗑️  Eliminada {catalogo}.golden.{tabla}")

# COMMAND ----------

# MAGIC %md ## 3. Eliminar tablas de la capa SILVER (creadas en 3.Transform.py)

# COMMAND ----------

tablas_silver = [
    "ecommerce_sales_cleaned",
    "instacart_orders_enriched",
]

for tabla in tablas_silver:
    spark.sql(f"DROP TABLE IF EXISTS `{catalogo}`.silver.{tabla}")
    print(f"🗑️  Eliminada {catalogo}.silver.{tabla}")

# COMMAND ----------

# MAGIC %md ## 4. Eliminar tablas de la capa BRONZE (creadas en 2.Ingest_Ecommerce.py / 2.Ingest_Instacart.py)

# COMMAND ----------

tablas_bronze = [
    "ecommerce_sales_raw",
    "aisles",
    "departments",
    "orders",
    "products",
    "order_products_prior",
    "order_products_train",
]

for tabla in tablas_bronze:
    spark.sql(f"DROP TABLE IF EXISTS `{catalogo}`.bronze.{tabla}")
    print(f"🗑️  Eliminada {catalogo}.bronze.{tabla}")

# COMMAND ----------

# MAGIC %md ## 5. Eliminar Esquemas y Catálogo (solo `catalog_proyecto_final`)

# COMMAND ----------

for esquema in ["raw", "bronze", "silver", "golden"]:
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalogo}`.{esquema} CASCADE")
    print(f"🗑️  Eliminado esquema {catalogo}.{esquema}")

spark.sql(f"DROP CATALOG IF EXISTS `{catalogo}` CASCADE")
print(f"🗑️  Eliminado catálogo {catalogo}")

# COMMAND ----------

# MAGIC %md ## 6. Eliminar SOLO la subcarpeta física "proyecto_final/"
# MAGIC Las tablas se crearon con `LOCATION` explícita dentro de
# MAGIC `bronze/proyecto_final/...`, `silver/proyecto_final/...` y
# MAGIC `golden/proyecto_final/...`, así que `DROP TABLE`/`DROP CATALOG` no
# MAGIC borra esos archivos automáticamente. Esto limpia solo esa subcarpeta
# MAGIC — **no toca** el resto de bronze/silver/golden donde vive tu ambiente
# MAGIC de prueba (`catalog_au`).

# COMMAND ----------

dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/proyecto_final/", True)
dbutils.fs.rm(f"abfss://silver@{storageName}.dfs.core.windows.net/proyecto_final/", True)
dbutils.fs.rm(f"abfss://golden@{storageName}.dfs.core.windows.net/proyecto_final/", True)

print("🗑️  Subcarpetas físicas 'proyecto_final/' eliminadas en bronze/silver/golden")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. External Locations y Storage Credential — **NO se eliminan aquí**
# MAGIC `exlt_raw`, `exlt_bronze`, `exlt_silver`, `exlt_golden`, `exlt_metastore-adb`
# MAGIC y la credencial `credential` son **infraestructura compartida** con tu
# MAGIC ambiente de prueba (`catalog_au`). Este rollback las deja intactas a
# MAGIC propósito. Solo bórralas manualmente si vas a desmontar TODO el
# MAGIC ambiente (prueba + proyecto final):
# MAGIC ```sql
# MAGIC DROP EXTERNAL LOCATION IF EXISTS exlt_raw;
# MAGIC DROP EXTERNAL LOCATION IF EXISTS exlt_bronze;
# MAGIC DROP EXTERNAL LOCATION IF EXISTS exlt_silver;
# MAGIC DROP EXTERNAL LOCATION IF EXISTS exlt_golden;
# MAGIC DROP EXTERNAL LOCATION IF EXISTS `exlt_metastore-adb`;
# MAGIC DROP STORAGE CREDENTIAL IF EXISTS credential;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. (Opcional, manual) Eliminar el Job/Workflow creado por GitHub Actions
# MAGIC Esto **no** se hace desde este notebook porque el Job vive fuera de
# MAGIC Unity Catalog. Ejecuta desde una terminal con Databricks CLI configurado,
# MAGIC o bórralo manualmente en **Databricks → Workflows → WF_Medallion_ProyectoFinal**:
# MAGIC ```
# MAGIC databricks jobs list --output json | jq '.jobs[] | select(.settings.name=="WF_Medallion_ProyectoFinal")'
# MAGIC databricks jobs delete --job-id <JOB_ID>
# MAGIC ```

# COMMAND ----------

print("🎉 Rollback completado. Verifica en Catalog Explorer que el catálogo")
print(f"   '{catalogo}' ya no aparece, y que 'catalog_au' sigue intacto.")
