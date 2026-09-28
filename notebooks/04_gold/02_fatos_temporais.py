# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · Fatos temporais
# MAGIC
# MAGIC | Tabela | Grão | Origem e técnica |
# MAGIC |---|---|---|
# MAGIC | `fato_disponibilidade_mensal` | anúncio × mês | `silver.calendario` **agregado por mês** (33 mi de linhas diárias → ~1,2 mi) |
# MAGIC | `fato_avaliacao` | avaliação | `silver.avaliacoes` **enriquecida** com anfitrião e bairro de `silver.anuncios` + `dim_bairro` |
# MAGIC
# MAGIC As duas se ligam à `dim_data` pela data e às demais dimensões pelas chaves do anúncio, do anfitrião e do bairro.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

def tabela_gold(nome):
    return f"{SCHEMA_GOLD}.{nome}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_disponibilidade_mensal
# MAGIC Cada anúncio tem 365 linhas por dia na Silver. Para as análises basta o resumo por mês, o que reduz o volume em ~28 vezes sem
# MAGIC perder a informação necessária. Lembrete (DQ-11): "indisponível" significa **reservado ou bloqueado** pelo anfitrião, e o
# MAGIC calendário descreve os próximos 12 meses, não o histórico. O primeiro e o último mês de cada cidade são parciais.

# COMMAND ----------

calendario = spark.table(tabela_silver("calendario"))
fato_disponibilidade = (calendario
    .groupBy("id_anuncio", "sigla_cidade", F.trunc("data", "month").alias("mes_referencia"))
    .agg(F.count("*").alias("dias_no_calendario"),
         F.sum(F.col("disponivel").cast("int")).alias("dias_disponiveis"),
         F.sum((~F.col("disponivel")).cast("int")).alias("dias_indisponiveis"))
    .withColumn("pct_dias_indisponiveis", F.round(F.col("dias_indisponiveis") / F.col("dias_no_calendario"), 4)))

destino = tabela_gold("fato_disponibilidade_mensal")
salvar_tabela(fato_disponibilidade, destino)
validar_chave(destino, ["id_anuncio", "mes_referencia"])
documentar_tabela(destino,
    "Gold | Disponibilidade futura dos anúncios por mês (próximos 12 meses a partir da coleta). Grão: 1 linha por anúncio e mês. "
    "Origem: silver.calendario agregado por mês. Indisponível = reservado ou bloqueado pelo anfitrião (DQ-11).",
    {
        "id_anuncio": "Anúncio; chave estrangeira para dim_imovel e fato_anuncio.",
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj.",
        "mes_referencia": "Primeiro dia do mês; chave estrangeira para dim_data. Domínio: 2026-06-01 a 2027-06-01.",
        "dias_no_calendario": "Dias do mês presentes no calendário (1 a 31; menor no primeiro e no último mês). Derivada: contagem.",
        "dias_disponiveis": "Dias livres para reserva no mês. Derivada: soma de silver.calendario.disponivel.",
        "dias_indisponiveis": "Dias reservados ou bloqueados no mês. Derivada.",
        "pct_dias_indisponiveis": "dias_indisponiveis / dias_no_calendario (0 a 1). Derivada.",
    })

display(spark.table(destino).groupBy("sigla_cidade", "mes_referencia").agg(
    F.count("*").alias("anuncios"),
    F.round(100 * F.sum("dias_indisponiveis") / F.sum("dias_no_calendario"), 1).alias("pct_indisponivel"),
).orderBy("sigla_cidade", "mes_referencia"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_avaliacao
# MAGIC Cada avaliação ganha o anfitrião e o bairro do anúncio avaliado, para que o volume e a recência da demanda possam ser
# MAGIC analisados direto por anfitrião ou por bairro, sem passar pela tabela de anúncios.

# COMMAND ----------

dim_bairro = spark.table(tabela_gold("dim_bairro")).select("sigla_cidade", "bairro", "sk_bairro")
contexto_anuncio = (spark.table(tabela_silver("anuncios"))
                    .select("id_anuncio", "id_anfitriao", "sigla_cidade", "bairro")
                    .join(dim_bairro, ["sigla_cidade", "bairro"], "left")
                    .select("id_anuncio", "id_anfitriao", "sk_bairro"))

fato_avaliacao = (spark.table(tabela_silver("avaliacoes"))
                  .join(contexto_anuncio, "id_anuncio", "inner")
                  .select("id_avaliacao", "id_anuncio", "id_anfitriao", "sk_bairro", "sigla_cidade", "data_avaliacao"))

destino = tabela_gold("fato_avaliacao")
salvar_tabela(fato_avaliacao, destino)
validar_chave(destino, ["id_avaliacao"])
print("Linhas na Silver:", f"{spark.table(tabela_silver('avaliacoes')).count():,}", "| na Gold:", f"{spark.table(destino).count():,}")
documentar_tabela(destino,
    "Gold | Avaliações de hóspedes (evento de demanda). Grão: 1 linha por avaliação. "
    "Origem: silver.avaliacoes + anfitrião e bairro de silver.anuncios e dim_bairro (JOIN por id_anuncio).",
    {
        "id_avaliacao": "Identificador da avaliação (chave). Origem: silver.avaliacoes.",
        "id_anuncio": "Anúncio avaliado; chave estrangeira para dim_imovel e fato_anuncio.",
        "id_anfitriao": "Anfitrião do anúncio; chave estrangeira para dim_anfitriao. Origem: JOIN com silver.anuncios.",
        "sk_bairro": "Bairro do anúncio; chave estrangeira para dim_bairro. Origem: JOIN com dim_bairro.",
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj.",
        "data_avaliacao": "Data da avaliação; chave estrangeira para dim_data. Domínio: 2010-06-07 até o fim da coleta.",
    })
