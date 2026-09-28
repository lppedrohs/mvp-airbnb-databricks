# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Avaliações
# MAGIC
# MAGIC `bronze.reviews_sp` + `bronze.reviews_rj` → **`silver.avaliacoes`** (1 linha por avaliação)
# MAGIC
# MAGIC | Etapa | O que é feito | Problemas tratados |
# MAGIC |---|---|---|
# MAGIC | 1. Centralização, tipagem e minimização | União SP + RJ; remoção de nome do avaliador e comentário | DQ-01, DQ-02, DQ-03, DQ-12 |
# MAGIC | 2. Filtros de validade | Chave nula, data impossível, anúncio inexistente, duplicados | DQ-13, DQ-14, DQ-15, DQ-17 |
# MAGIC
# MAGIC A tabela guarda apenas **quando** e **em qual anúncio** houve avaliação — é o que as perguntas precisam para medir volume e
# MAGIC recência de demanda por anúncio e por anfitrião.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

bronze = ler_bronze_unificada("reviews")
qtd_bronze = bronze.count()
print(f"Avaliações na Bronze (SP + RJ): {qtd_bronze:,}")

avaliacoes = bronze.select(                        # reviewer_id, reviewer_name e comments descartados (DQ-12)
    para_bigint("id").alias("id_avaliacao"),
    para_bigint("listing_id").alias("id_anuncio"),
    F.col("_cidade").alias("sigla_cidade"),
    para_data("date").alias("data_avaliacao"),
    F.col("_data_snapshot").alias("data_snapshot"),
)

# COMMAND ----------

etapa_1 = avaliacoes.filter(F.col("id_avaliacao").isNotNull() & F.col("id_anuncio").isNotNull())
qtd_1 = etapa_1.count()
registrar_impacto("Remove avaliação sem id ou sem anúncio (DQ-14)", qtd_bronze, qtd_1)

# O Airbnb foi fundado em 2008; avaliações antes disso ou depois do fim da coleta são impossíveis.
# O limite superior é a última data de coleta da cidade, e não a data do snapshot: a coleta dura dias (DQ-17).
fim_da_coleta = (spark.table(tabela_silver("anuncios"))
                 .groupBy("sigla_cidade").agg(F.max("data_coleta").alias("fim_da_coleta")))
display(fim_da_coleta.orderBy("sigla_cidade"))

etapa_2 = (etapa_1.join(F.broadcast(fim_da_coleta), "sigla_cidade", "left")
           .filter(F.col("data_avaliacao").between(F.lit("2008-01-01").cast("date"), F.col("fim_da_coleta")))
           .drop("fim_da_coleta"))
qtd_2 = etapa_2.count()
registrar_impacto("Remove data nula, anterior a 2008 ou após o fim da coleta (DQ-15, DQ-17)", qtd_1, qtd_2)

etapa_3 = etapa_2.join(spark.table(tabela_silver("anuncios")).select("id_anuncio"), "id_anuncio", "left_semi")
qtd_3 = etapa_3.count()
registrar_impacto("Remove avaliação de anúncio inexistente em silver.anuncios (DQ-13)", qtd_2, qtd_3)

etapa_4 = etapa_3.dropDuplicates(["id_avaliacao"]).select(*avaliacoes.columns)
qtd_4 = etapa_4.count()
registrar_impacto("Remove duplicados por id_avaliacao (DQ-14)", qtd_3, qtd_4)

# COMMAND ----------

# MAGIC %md
# MAGIC Nenhuma avaliação foi removida pela regra de datas. Se o limite fosse a data do snapshot, 299 avaliações válidas (192 em SP e
# MAGIC 107 no RJ) teriam sido descartadas (DQ-17). As 1.643 remoções vêm das avaliações dos 35 anúncios órfãos (DQ-13).

# COMMAND ----------

destino = tabela_silver("avaliacoes")
salvar_tabela(etapa_4, destino)
validar_chave(destino, ["id_avaliacao"])

documentar_tabela(destino,
    "Silver | Avaliações de hóspedes dos anúncios de SP e RJ. Grão: 1 linha por avaliação (id_avaliacao). "
    "Origem: bronze.reviews_sp e bronze.reviews_rj (união). Nome do avaliador, id do avaliador e comentário removidos "
    "por minimização de dados pessoais (DQ-12). Somente anúncios presentes em silver.anuncios.",
    {
        "id_avaliacao": "Identificador da avaliação (chave primária). BIGINT convertido de texto (DQ-02). Origem: reviews.id",
        "id_anuncio": "Anúncio avaliado; chave estrangeira para silver.anuncios. Origem: reviews.listing_id",
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze.",
        "data_avaliacao": "Data da avaliação. Domínio: 2008-01-01 até a última data de coleta da cidade (DQ-17). Origem: reviews.date",
        "data_snapshot": "Data do snapshot da cidade na fonte. Origem: metadado _data_snapshot da Bronze.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC Cobertura temporal das avaliações — define quais janelas de recência são confiáveis para as métricas da Gold.

# COMMAND ----------

display(spark.table(destino)
        .groupBy("sigla_cidade", F.year("data_avaliacao").alias("ano"))
        .count()
        .orderBy("sigla_cidade", "ano"))
display(spark.sql(f"DESCRIBE TABLE {destino}"))
