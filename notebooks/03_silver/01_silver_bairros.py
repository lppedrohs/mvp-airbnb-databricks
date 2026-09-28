# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Bairros
# MAGIC
# MAGIC `bronze.neighbourhoods_sp` + `bronze.neighbourhoods_rj` → **`silver.bairros`** (1 linha por cidade + bairro)
# MAGIC
# MAGIC Lista oficial de bairros usada pela fonte (96 em SP e 160 no RJ). É a referência para validar o bairro de cada anúncio (DQ-13) e
# MAGIC será a base da dimensão de localização na Gold. O agrupamento de bairros existe só em SP (32 subprefeituras) e é mantido como
# MAGIC atributo opcional `grupo_bairro` (DQ-04).

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

bronze = ler_bronze_unificada("neighbourhoods")   # centralização SP + RJ (DQ-03)
qtd_bronze = bronze.count()

bairros = (bronze
           .select(F.col("_cidade").alias("sigla_cidade"),
                   col_nome_cidade().alias("cidade"),
                   F.trim("neighbourhood").alias("bairro"),
                   F.trim("neighbourhood_group").alias("grupo_bairro"),  # DQ-04: preenchido só em SP (subprefeituras)
                   F.col("_data_snapshot").alias("data_snapshot"))
           .filter(F.col("bairro").isNotNull() & (F.col("bairro") != ""))
           .dropDuplicates(["sigla_cidade", "bairro"]))                 # DQ-14

registrar_impacto("Bairros sem nome + duplicados removidos", qtd_bronze, bairros.count())

# COMMAND ----------

destino = tabela_silver("bairros")
salvar_tabela(bairros, destino)
validar_chave(destino, ["sigla_cidade", "bairro"])

documentar_tabela(destino,
    "Silver | Lista oficial de bairros de São Paulo e Rio de Janeiro usada pelo Inside Airbnb. "
    "Grão: 1 linha por cidade + bairro. Origem: bronze.neighbourhoods_sp e bronze.neighbourhoods_rj (união). "
    "grupo_bairro preenchido apenas em SP, com a subprefeitura; NULL no RJ (DQ-04).",
    {
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze.",
        "cidade": "Nome da cidade. Domínio: São Paulo, Rio de Janeiro. Derivado de sigla_cidade.",
        "bairro": "Nome oficial do bairro (espaços extras removidos). Origem: neighbourhoods.neighbourhood.",
        "grupo_bairro": "Agrupamento de bairros: subprefeitura em SP (32 valores); NULL no RJ, onde a fonte não publica. Origem: neighbourhoods.neighbourhood_group.",
        "data_snapshot": "Data de coleta do snapshot na fonte. Origem: metadado _data_snapshot da Bronze.",
    })

# COMMAND ----------

display(spark.table(destino).groupBy("cidade").agg(F.count("*").alias("bairros"), F.countDistinct("grupo_bairro").alias("grupos")).orderBy("cidade"))
display(spark.sql(f"DESCRIBE TABLE {destino}"))
