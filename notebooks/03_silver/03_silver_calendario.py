# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Calendário
# MAGIC
# MAGIC `bronze.calendar_sp` + `bronze.calendar_rj` → **`silver.calendario`** (1 linha por anúncio × dia)
# MAGIC
# MAGIC | Etapa | O que é feito | Problemas tratados |
# MAGIC |---|---|---|
# MAGIC | 1. Centralização e tipagem | União SP + RJ, datas e booleanos convertidos | DQ-01, DQ-02, DQ-03 |
# MAGIC | 2. Preço do calendário | Verifica se a fonte publica preço diário (nesta versão não publica) | DQ-10 |
# MAGIC | 3. Filtros de validade | Chave nula, disponibilidade inválida, anúncio inexistente, duplicados | DQ-13, DQ-14 |
# MAGIC
# MAGIC Limitação registrada (DQ-11): o calendário mostra os **próximos 365 dias** e `disponivel = false` significa *reservado ou
# MAGIC bloqueado pelo anfitrião* — a fonte não diferencia os dois casos.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

bronze = ler_bronze_unificada("calendar")
qtd_bronze = bronze.count()

# DQ-10: a versão atual da fonte não publica preço no calendário. A checagem evita quebrar o pipeline
# e reaproveita o preço automaticamente caso a coluna volte a existir e esteja preenchida.
USAR_PRECO_CALENDARIO = False
if "price" in bronze.columns:
    pct_preco_vazio = bronze.select(
        F.avg(F.when(F.col("price").isNull() | (F.trim("price") == ""), 1.0).otherwise(0.0))).first()[0] * 100
    USAR_PRECO_CALENDARIO = pct_preco_vazio < 50
    print(f"price vazio no calendário: {pct_preco_vazio:.1f}%")
print(f"Linhas na Bronze (SP + RJ): {qtd_bronze:,}")
print("Preço diário no calendário:",
      "mantido como preco_diaria" if USAR_PRECO_CALENDARIO else "indisponível na fonte; a diária vem de silver.anuncios")

# COMMAND ----------

calendario = bronze.select(
    para_bigint("listing_id").alias("id_anuncio"),
    F.col("_cidade").alias("sigla_cidade"),
    para_data("date").alias("data"),
    parse_booleano("available").alias("disponivel"),
    para_inteiro("minimum_nights").alias("noites_minimas"),
    para_inteiro("maximum_nights").alias("noites_maximas"),
    *([parse_preco("price").alias("preco_diaria")] if USAR_PRECO_CALENDARIO else []),
    F.col("_data_snapshot").alias("data_snapshot"),
)

# COMMAND ----------

etapa_1 = calendario.filter(F.col("id_anuncio").isNotNull() & F.col("data").isNotNull())
qtd_1 = etapa_1.count()
registrar_impacto("Remove linha sem id_anuncio ou data (DQ-14)", qtd_bronze, qtd_1)

etapa_2 = etapa_1.filter(F.col("disponivel").isNotNull())
qtd_2 = etapa_2.count()
registrar_impacto("Remove disponibilidade fora do domínio t/f (DQ-01)", qtd_1, qtd_2)

etapa_3 = etapa_2.join(spark.table(tabela_silver("anuncios")).select("id_anuncio"), "id_anuncio", "left_semi")
qtd_3 = etapa_3.count()
registrar_impacto("Remove dia de anúncio inexistente em silver.anuncios (DQ-13)", qtd_2, qtd_3)

etapa_4 = etapa_3.dropDuplicates(["id_anuncio", "data"]).select(*calendario.columns)
qtd_4 = etapa_4.count()
registrar_impacto("Remove duplicados por (id_anuncio, data) (DQ-14)", qtd_3, qtd_4)

# COMMAND ----------

# MAGIC %md
# MAGIC Só a regra de integridade removeu linhas: 14.600 dias = os 40 anúncios órfãos do diagnóstico × 365 dias (DQ-13).

# COMMAND ----------

destino = tabela_silver("calendario")
salvar_tabela(etapa_4, destino)
validar_chave(destino, ["id_anuncio", "data"])

colunas_doc = {
    "id_anuncio": "Identificador do anúncio; chave estrangeira para silver.anuncios. Origem: calendar.listing_id",
    "sigla_cidade": "Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze.",
    "data": "Dia do calendário. Domínio: 365 dias a partir da coleta (SP: 2026-06-14 a 2027-06-15; RJ: 2026-06-25 a 2027-06-30). Origem: calendar.date",
    "disponivel": "true = dia livre para reserva; false = reservado OU bloqueado pelo anfitrião (a fonte não diferencia, DQ-11). Origem: calendar.available (t/f)",
    "noites_minimas": "Estadia mínima exigida para check-in neste dia. Origem: calendar.minimum_nights",
    "noites_maximas": "Estadia máxima permitida para check-in neste dia. Origem: calendar.maximum_nights",
    "data_snapshot": "Data do snapshot da cidade na fonte. Origem: metadado _data_snapshot da Bronze.",
}
if USAR_PRECO_CALENDARIO:
    colunas_doc["preco_diaria"] = "Preço da diária neste dia, em BRL. Origem: calendar.price"

documentar_tabela(destino,
    "Silver | Calendário de disponibilidade dos anúncios de SP e RJ para os 365 dias seguintes ao snapshot. "
    "Grão: 1 linha por anúncio e dia (id_anuncio, data). Origem: bronze.calendar_sp e bronze.calendar_rj (união). "
    "Somente anúncios presentes em silver.anuncios. A fonte não publica preço diário no calendário (DQ-10); a diária está em silver.anuncios.",
    colunas_doc)

# COMMAND ----------

display(spark.table(destino).groupBy("sigla_cidade").agg(
    F.min("data").alias("data_inicial"),
    F.max("data").alias("data_final"),
    F.countDistinct("id_anuncio").alias("anuncios"),
    F.round(100 * F.avg(F.col("disponivel").cast("int")), 1).alias("pct_dias_disponiveis"),
).orderBy("sigla_cidade"))
display(spark.sql(f"DESCRIBE TABLE {destino}"))
