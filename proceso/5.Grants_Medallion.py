# Databricks notebook source
# MAGIC %md
# MAGIC ## Grants - Proyecto Final
# MAGIC Sigue el mismo patrón que tu `5.Grants_Medallion` de prueba: sentencias
# MAGIC `GRANT` directas por `%sql`. Ajusta los correos/grupos
# MAGIC (`analista1@hotmail.com`, `Devops`, etc.) a los principals reales de tu
# MAGIC workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog_proyecto_final TO `analista1@hotmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.bronze TO `analista1@hotmail.com`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.bronze TO `Data Analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.silver TO `dev1@hotmail.com`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.silver TO `Devops`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT CREATE TABLE ON SCHEMA catalog_proyecto_final.bronze TO `admin2@hotmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.ecommerce_sales_raw TO `analista2@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.aisles TO `dev2@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.departments TO `analista3@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.orders TO `analista1@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.products TO `dev1@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.order_products_prior TO `analista2@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.bronze.order_products_train TO `devops1@hotmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog_proyecto_final TO `Developers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.golden TO `Developers`;
# MAGIC GRANT USE CATALOG ON CATALOG catalog_proyecto_final TO `Data Analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.golden TO `Data Analysts`;
# MAGIC GRANT USE CATALOG ON CATALOG catalog_proyecto_final TO `Devops`;
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proyecto_final.golden TO `Devops`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.golden.kpi_ventas_ecommerce TO `Developers`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.golden.kpi_pedidos_por_departamento_aisle TO `Data Analysts`;
# MAGIC GRANT SELECT ON TABLE catalog_proyecto_final.golden.kpi_tasa_recompra_producto TO `Devops`;