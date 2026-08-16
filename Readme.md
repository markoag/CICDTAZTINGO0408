# Proyecto Final — Ingeniería de Datos con Databricks

Plataforma de datos end-to-end con **Arquitectura Medallion** (Bronze / Silver / Golden) sobre **Azure Databricks + Unity Catalog**, que integra dos fuentes de datos de Kaggle — **Ecommerce Sales Prediction** e **Instacart Market Basket Analysis** — automatiza su despliegue y orquestación con **CI/CD (GitHub Actions)**, expone los resultados en **dashboards interactivos de Power BI**, y suma una **Databricks App** conectada en vivo a **Lakebase Postgres** para consulta y edición operativa de datos.

**Stack:** Python · PySpark · Azure Databricks · Delta Lake · Unity Catalog · Azure Data Lake Storage Gen2 (ADLS Gen2) · Managed Identity · GitHub Actions · Power BI · Dash · Lakebase Postgres (Autoscaling)

---

## ¿Qué preguntas responde este proyecto?

**Ventas Ecommerce (dashboard Resumen Ejecutivo):**

- ¿Cuáles son los ingresos totales y unidades vendidas por categoría de producto?
- ¿Qué categoría genera más ingresos y cuál tiene el ticket promedio más alto?
- ¿Cómo se distribuye la facturación entre Electronics, Fashion, Home Decor, Sports y Toys?

**Instacart — Departamentos y Pasillos (dashboard Departamentos):**

- ¿Qué departamentos y pasillos (aisles) concentran más pedidos?
- ¿Cuántos productos distintos se mueven por departamento?
- ¿Cómo se distribuye jerárquicamente el catálogo (departamento → pasillo → producto)?

**Instacart — Recompra (dashboard Top Productos y Recompra):**

- ¿Qué productos tienen mayor tasa de recompra?
- ¿Cómo se relaciona el volumen de pedidos con la fidelidad de recompra por producto?

**Operación de datos (Databricks App sobre Lakebase Postgres):**

- ¿Cómo consultar y editar en vivo los registros de `aisles`, `departments`, `orders` y `ecommerce_sales_prediction_dataset` sin pasar por un notebook?

---

## Arquitectura

```
Kaggle: Ecommerce Sales Prediction + Instacart Market Basket
        │  Carga manual de 8 archivos CSV
        ▼
   RAW — ADLS Gen2 (adstaztingo0408prod)
        │  GitHub Actions → databricks workspace import_dir
        ▼
   Notebooks desplegados en Producción (/smartdata/proceso)
        │  Databricks Workflow "WF_Medallion_ProyectoFinal" · Cluster_SD
        ▼
   BRONZE — Delta Lake      ← ingesta cruda + metadatos de trazabilidad (7 tablas)
        │  3.Transform.py
        ▼
   SILVER — Delta Lake      ← limpieza, casteo, joins, deduplicación (2 tablas)
        │  4.Load.py
        ▼
   GOLDEN — Delta Lake      ← KPIs agregados listos para consumo (3 tablas)
        │  Conector Databricks           │  Lakebase Postgres
        ▼                                ▼
   Power BI (3 dashboards)         Databricks App (Dash + CRUD)
```

Todo el flujo, desde el `git push`/merge a `main` hasta la ejecución del pipeline completo en Producción, corre sin intervención manual.

---

## Infraestructura Azure desplegada

Todos los recursos viven en el resource group **`rg-taztingogr`**:

![1786842384336](image/Readme/1786842384336.png)

| Recurso                   | Nombre                                   | Rol en el proyecto                                                                              |
| ------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Managed Identity          | `miadbtaztingo0408`                    | Autentica Unity Catalog contra ADLS Gen2 (Storage Credential)                                   |
| Storage Account           | `adstaztingo0408prod`                  | Data Lake con contenedores`raw`, `bronze`, `silver`, `golden`, `metastore-adb`        |
| Azure Databricks Service  | `adbtaztingo0408prod`                  | Workspace de Producción — ejecuta el Workflow Medallion                                       |
| Access Connector          | `ac-taztingo`                          | Vínculo entre la Managed Identity y Databricks (Unity Catalog External Locations)              |
| SQL Server / SQL Database | `servertaztingo` / `dbtaztingo`      | Infraestructura reservada del landing zone para extensiones futuras (plano de control)          |
| Key Vault, Data Factory   | `akvtaztingo0608`, `adftaztingo0508` | Recursos compartidos de la plataforma, no utilizados activamente en el alcance de este proyecto |

**Managed Identity** — usada por la Storage Credential de Unity Catalog para autenticar contra ADLS Gen2 sin credenciales estáticas:

![1786842411388](image/Readme/1786842411388.png)

**Storage Account** — Data Lake Gen2 con la jerarquía de contenedores de la arquitectura Medallion:

![1786842448403](image/Readme/1786842448403.png)

**Azure Databricks Service (Producción)** — workspace donde corre el Workflow `WF_Medallion_ProyectoFinal`:

![1786842480129](image/Readme/1786842480129.png)

**SQL Server / SQL Database** — provisionados como parte del landing zone, disponibles para un futuro plano de control (parametrización, logs de ejecución) al estilo del proyecto de referencia:

![1786842494814](image/Readme/1786842494814.png)

![1786842504688](image/Readme/1786842504688.png)

---

## CI/CD — GitHub Actions

Repositorio: **`markoag/CICDTAZTINGO0408`**. Cada *merge* de Pull Request a `main` dispara automáticamente el workflow **"CI/CD Pipeline - Databricks Medallion Proyecto Final"**, que:

1. Despliega los notebooks de `proceso/` al workspace de Producción.
2. Elimina el Workflow anterior en Databricks si existe (evita duplicados).
3. Localiza el clúster `Cluster_SD` (debe estar encendido).
4. Crea el Workflow `WF_Medallion_ProyectoFinal` con las 6 tareas encadenadas.
5. Valida su configuración, lo ejecuta y monitorea hasta que termina.
6. Limpia archivos temporales y reporta el resultado.

![1786842673955](image/Readme/1786842673955.png)

Historial de ejecuciones — cada PR mergeada queda registrada con su resultado:

![1786842689707](image/Readme/1786842689707.png)

---

## Databricks Workflow — `WF_Medallion_ProyectoFinal`

Orquesta las 6 tareas de la arquitectura Medallion sobre el clúster **`Cluster_SD`** (Single node, `Standard_D4plds_v6`, Databricks Runtime 17.3 LTS):

```
prepambiente ─┬─► ingest_ecommerce ─┐
              └─► ingest_instacart ─┴─► transform ─► load ─► grants
```

Última ejecución exitosa: **8m 23s**, las 6 tareas en verde.

![1786842700571](image/Readme/1786842700571.png)

Vista de corridas y duración por tarea:

![1786842710949](image/Readme/1786842710949.png)

---

## Unity Catalog y Delta Sharing

El catálogo **`catalog_proyecto_final`** organiza las capas Bronze/Silver/Golden con permisos RBAC diferenciados por rol (`DEs` sobre bronze/silver, `DAs` sobre golden). Adicionalmente, se habilitó **Delta Sharing** creando un *recipient* externo, evidenciando la capacidad de compartir datos de forma segura fuera del workspace sin duplicar archivos:

![1786842731312](image/Readme/1786842731312.png)

---

## Dashboards Power BI

### RESUMEN EJECUTIVO — Ventas Ecommerce

KPIs globales: **$11.24 millones** en ingresos totales, **30 mil** unidades vendidas, **231 mil** órdenes totales y **$2.53 mil** de precio promedio. El donut y la tabla desglosan ingresos, unidades y precio promedio por categoría (`Electronics`, `Fashion`, `Home Decor`, `Sports`, `Toys`), con una distribución equilibrada entre las 5 categorías (cada una entre ~19% y ~21% del total).

![1786842752737](image/Readme/1786842752737.png)

---

### DEPARTAMENTOS — Instacart

Treemap jerárquico `department → aisle` sobre el total de productos pedidos, más una tabla de soporte con el total de órdenes por departamento. **`produce`** y **`dairy eggs`** encabezan el volumen (48,571 y 42,376 órdenes respectivamente), sobre un total de **231,215** órdenes analizadas, sumando **25 mil** productos distintos distribuidos en **22 departamentos** y **135 pasillos (aisles)**.

![1786842760567](image/Readme/1786842760567.png)

---

### TOP PRODUCTOS Y RECOMPRA — Instacart

Ranking de productos por tasa de recompra (`Tasa_Recompra`) y una vista de dispersión que cruza volumen de pedidos (`Veces_Pedido`) contra tasa de recompra, con un slicer de búsqueda por `product_name` para exploración libre del catálogo.

![1786842768305](image/Readme/1786842768305.png)

---

## Databricks App — Explorador de Datos sobre Lakebase Postgres

Aplicación construida con **Dash** que se conecta **directamente a Lakebase Postgres** (no a SQL Warehouse/Unity Catalog) usando el patrón oficial de Databricks: rotación automática de token OAuth vía `WorkspaceClient().postgres.generate_database_credential()` + `psycopg_pool.ConnectionPool`.

Permite explorar y editar en vivo las tablas `aisles`, `departments`, `orders` y `ecommerce_sales_prediction_dataset`:

- Detección dinámica de columnas vía `information_schema` (no depende de nombres fijos).
- Filtros dropdown generados automáticamente sobre columnas categóricas de baja cardinalidad.
- Edición y borrado de registros usando `ctid` como clave, funcionando incluso en tablas sin llave primaria definida.

Configuración de permisos, variables de entorno y despliegue documentadas en `databricks-app/SETUP_LAKEBASE.md`.

---

## Datos del proyecto

| Métrica                                | Valor                                                                                                 |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Fuentes de datos                        | 2 (Kaggle: Ecommerce Sales Prediction + Instacart Market Basket)                                      |
| Archivos CSV en`raw`                  | 8 (7 usados en el ETL +`sample_submission.csv` de referencia)                                       |
| Tablas Bronze                           | 7                                                                                                     |
| Tablas Silver                           | 2                                                                                                     |
| Tablas Golden (KPIs)                    | 3                                                                                                     |
| Tareas del Workflow Medallion           | 6 (`prepambiente`, `ingest_ecommerce`, `ingest_instacart`, `transform`, `load`, `grants`) |
| Cluster de Producción                  | `Cluster_SD` · Standard_D4plds_v6 · DBR 17.3 LTS                                                  |
| Duración última corrida exitosa       | 8m 23s (Databricks) · 8m 59s (pipeline GitHub Actions completo)                                      |
| Ingresos totales analizados (Ecommerce) | $11.24 millones                                                                                       |
| Unidades vendidas (Ecommerce)           | 30,000+                                                                                               |
| Órdenes totales analizadas (Instacart) | 231,215                                                                                               |
| Departamentos / Pasillos (Instacart)    | 22 / 135                                                                                              |
| Productos distintos (Instacart)         | ~25,000                                                                                               |
| Dashboards Power BI                     | 3 páginas (Resumen Ejecutivo, Departamentos, Top Productos y Recompra)                               |
| Apps Databricks                         | 1 (Explorador Lakebase Postgres, Dash)                                                                |

---

## Estructura del repositorio

```
proyecto-final-databricks/
│
│  ── CI/CD ──────────────────────────────────────────────────────
├── .github/
│   └── workflows/
│       └── deploy-notebook.yml       ← Pipeline CI/CD completo (9 pasos)
│
│  ── DATASETS ────────────────────────────────────────────────────
├── datasets/                         ← CSV fuente (Kaggle)
│   ├── Ecommerce_Sales_Prediction_Dataset.csv
│   ├── aisles.csv
│   ├── departments.csv
│   ├── order_products__prior.csv
│   ├── order_products__train.csv
│   ├── orders.csv
│   ├── products.csv
│   └── sample_submission.csv
│
│  ── PROCESO — Notebooks ETL PySpark (Pipeline en Producción) ───
├── proceso/
│   ├── 1.Preparacion_Ambiente.py     ← Storage credential, external locations, catálogo
│   ├── 2.Ingest_Ecommerce.py         ← raw -> bronze (Ecommerce)
│   ├── 2.Ingest_Instacart.py         ← raw -> bronze (6 tablas Instacart)
│   ├── 3.Transform.py                ← bronze -> silver
│   ├── 4.Load.py                     ← silver -> golden (KPIs)
│   └── 5.Grants_Medallion.py         ← RBAC sobre catálogo/esquemas
│
│  ── SEGURIDAD / PREPARACIÓN / REVERSIÓN ─────────────────────────
├── PrepAmb/
│   └── 1.Preparacion_Ambiente.sql
├── seguridad/
│   └── 5.Grants_Medallion.sql
├── reversion/
│   └── rollback_environment.py       ← Notebook de rollback del ambiente
│
│  ── APP — Databricks App sobre Lakebase Postgres ────────────────
├── databricks-app/
│   ├── app.py
│   ├── app.yaml
│   ├── manifest.yaml
│   ├── requirements.txt
│   └── SETUP_LAKEBASE.md
│
│  ── DASHBOARD ───────────────────────────────────────────────────
├── dashboard/
│   ├── Ventas_ecommerce_pfinal.pbix
│   └── enlace_powerbi.txt
│
│  ── EVIDENCIAS Y DOCUMENTACIÓN ──────────────────────────────────
├── certificaciones/
├── evidencias/
├── docs/
│   └── imagenes/
│       ├── azure/          ← Managed Identity, Storage, Databricks Service, SQL
│       ├── cicd/           ← GitHub Actions y Databricks Workflow
│       ├── unity_catalog/  ← Delta Sharing
│       └── powerbi/        ← Los 3 dashboards
└── README.md
```

---

*Desarrollado por Vinicio Lapo — Proyecto Final de Ingeniería de Datos con Databricks · 2026*
