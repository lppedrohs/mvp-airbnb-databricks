# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Estrutura no Unity Catalog
# MAGIC
# MAGIC Cria um **schema por camada** da arquitetura medalhão e o **Volume** que recebe os arquivos originais da fonte.
# MAGIC Todos os comandos usam `IF NOT EXISTS`, então o notebook pode ser executado novamente sem efeito colateral.
# MAGIC
# MAGIC ```
# MAGIC workspace (catálogo)
# MAGIC ├── bronze   → volume raw_files (arquivos .csv/.csv.gz) + tabelas brutas por cidade
# MAGIC ├── silver   → tabelas limpas, tipadas e centralizadas (SP + RJ)
# MAGIC └── gold     → modelo dimensional (fatos e dimensões) para as análises
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

camadas = {
    SCHEMA_BRONZE: "Camada Bronze: arquivos e tabelas brutas do Inside Airbnb (SP e RJ), sem transformação, com metadados de ingestão.",
    SCHEMA_SILVER: "Camada Silver: dados limpos, tipados, deduplicados e centralizados (SP + RJ), com flags de qualidade.",
    SCHEMA_GOLD:   "Camada Gold: modelo dimensional (esquema estrela) pronto para responder as perguntas de negócio.",
}
for schema, descricao in camadas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{descricao}'")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.raw_files
    COMMENT 'Arquivos originais baixados do Inside Airbnb (CC BY 4.0), organizados em <cidade>/<data do snapshot>/'
""")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))
display(spark.sql(f"SHOW VOLUMES IN {SCHEMA_BRONZE}"))
