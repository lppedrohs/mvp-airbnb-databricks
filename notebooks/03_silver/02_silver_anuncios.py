# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Anúncios
# MAGIC
# MAGIC `bronze.listings_sp` + `bronze.listings_rj` → **`silver.anuncios`** (1 linha por anúncio)
# MAGIC
# MAGIC | Etapa | O que é feito | Problemas tratados |
# MAGIC |---|---|---|
# MAGIC | 1. Centralização | União SP + RJ por nome de coluna | DQ-03 |
# MAGIC | 2. Seleção e tipagem | Só colunas úteis às perguntas, nomes em português, tipos corretos | DQ-01, DQ-02, DQ-04, DQ-09, DQ-16 |
# MAGIC | 3. Filtros de validade | Remove registros sem chave, fora da cidade, capacidade inválida e duplicados | DQ-14, DQ-15 |
# MAGIC | 4. Flags analíticas | Marca (sem remover) preço ausente, outlier, estadia longa e inatividade | DQ-05, DQ-06, DQ-07 |
# MAGIC | 5. Persistência | Delta + validação de chave + catálogo | — |
# MAGIC
# MAGIC Regra adotada: **a Silver remove apenas registros inválidos**. Registros válidos, mas inadequados para uma análise específica,
# MAGIC recebem uma flag — assim a decisão de filtrar fica explícita na Gold e nenhuma informação é perdida.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Centralização SP + RJ

# COMMAND ----------

bronze = ler_bronze_unificada("listings")
qtd_bronze = bronze.count()
print(f"Anúncios na Bronze (SP + RJ): {qtd_bronze:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seleção, renomeação e tipagem
# MAGIC Das ~80 colunas da fonte ficam apenas as necessárias para as três perguntas. Textos longos (descrição, URLs, fotos) e dados
# MAGIC pessoais do anfitrião (nome, "sobre mim") são descartados, assim como as 11 colunas que a fonte publica vazias (DQ-16).
# MAGIC Colunas que só existem nas versões mais recentes da fonte (estimativas de ocupação e receita) são tratadas como opcionais.

# COMMAND ----------

def se_existir(coluna, expressao, tipo):
    return expressao if coluna in bronze.columns else F.lit(None).cast(tipo)


comodidades = F.from_json("amenities", "array<string>")

anuncios = bronze.select(
    para_bigint("id").alias("id_anuncio"),                                   # DQ-02
    para_bigint("host_id").alias("id_anfitriao"),
    F.col("_cidade").alias("sigla_cidade"),
    col_nome_cidade().alias("cidade"),
    F.trim("neighbourhood_cleansed").alias("bairro"),                        # DQ-04
    para_double("latitude").alias("latitude"),
    para_double("longitude").alias("longitude"),
    F.trim("property_type").alias("tipo_propriedade"),
    F.trim("room_type").alias("tipo_quarto"),
    para_inteiro("accommodates").alias("capacidade_hospedes"),
    para_inteiro("bedrooms").alias("quartos"),
    para_inteiro("beds").alias("camas"),
    se_existir("bathrooms", para_double("bathrooms"), "double").alias("banheiros"),
    F.when(comodidades.isNotNull(), F.size(comodidades)).alias("qtd_comodidades"),
    parse_preco("price").alias("preco_diaria"),                              # DQ-01
    para_inteiro("minimum_nights").alias("noites_minimas"),
    para_inteiro("maximum_nights").alias("noites_maximas"),
    para_inteiro("availability_365").alias("dias_disponiveis_365"),
    se_existir("estimated_occupancy_l365d", para_inteiro("estimated_occupancy_l365d"), "int").alias("noites_ocupadas_estimadas_12m"),
    se_existir("estimated_revenue_l365d", para_double("estimated_revenue_l365d"), "double").alias("receita_estimada_12m"),
    para_inteiro("number_of_reviews").alias("num_avaliacoes"),
    para_inteiro("number_of_reviews_ltm").alias("num_avaliacoes_12m"),
    para_double("reviews_per_month").alias("avaliacoes_por_mes"),
    para_data("first_review").alias("data_primeira_avaliacao"),
    para_data("last_review").alias("data_ultima_avaliacao"),
    para_double("review_scores_rating").alias("nota_geral"),                 # DQ-08: NULL mantido
    para_double("review_scores_cleanliness").alias("nota_limpeza"),
    para_double("review_scores_location").alias("nota_localizacao"),
    para_double("review_scores_value").alias("nota_custo_beneficio"),
    se_existir("hosts_time_as_host_years",                                   # DQ-16: substitui host_since (vazio)
               para_inteiro("hosts_time_as_host_years") * 12 + F.coalesce(para_inteiro("hosts_time_as_host_months"), F.lit(0)),
               "int").alias("meses_como_anfitriao"),
    parse_booleano("host_is_superhost").alias("anfitriao_superhost"),
    parse_booleano("host_identity_verified").alias("anfitriao_identidade_verificada"),
    para_inteiro("calculated_host_listings_count").alias("qtd_anuncios_anfitriao_cidade"),  # DQ-09
    para_data("last_scraped").alias("data_coleta"),
    F.col("_data_snapshot").alias("data_snapshot"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC Checagem das conversões (DQ-01): valores preenchidos na fonte que viraram NULL após o `try_cast`. O esperado é zero —
# MAGIC confirma que as regras cobrem todos os formatos encontrados no diagnóstico.

# COMMAND ----------

def falhas(coluna_fonte, expressao):
    return F.sum(F.when(F.col(coluna_fonte).isNotNull() & (F.trim(coluna_fonte) != "") & expressao.isNull(), 1).otherwise(0))


display(bronze.select(
    falhas("id", para_bigint("id")).alias("id"),
    falhas("host_id", para_bigint("host_id")).alias("host_id"),
    falhas("price", parse_preco("price")).alias("price"),
    falhas("latitude", para_double("latitude")).alias("latitude"),
    falhas("accommodates", para_inteiro("accommodates")).alias("accommodates"),
    falhas("last_scraped", para_data("last_scraped")).alias("last_scraped"),
    falhas("host_is_superhost", parse_booleano("host_is_superhost")).alias("host_is_superhost"),
    falhas("amenities", comodidades).alias("amenities"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Filtros de validade (remoção)

# COMMAND ----------

dentro_da_cidade = F.lit(False)
for sigla, cidade in CIDADES.items():
    lat_min, lat_max, lon_min, lon_max = cidade["bbox"]
    dentro_da_cidade = dentro_da_cidade | (
        (F.col("sigla_cidade") == sigla)
        & F.col("latitude").between(lat_min, lat_max)
        & F.col("longitude").between(lon_min, lon_max))

etapa_1 = anuncios.filter(F.col("id_anuncio").isNotNull() & F.col("id_anfitriao").isNotNull())
qtd_1 = etapa_1.count()
registrar_impacto("Remove anúncio sem id ou sem anfitrião (DQ-14)", qtd_bronze, qtd_1)

etapa_2 = etapa_1.filter(dentro_da_cidade)
qtd_2 = etapa_2.count()
registrar_impacto("Remove coordenada fora do município (DQ-15)", qtd_1, qtd_2)

etapa_3 = etapa_2.filter(F.col("capacidade_hospedes") > 0)
qtd_3 = etapa_3.count()
registrar_impacto("Remove capacidade de hóspedes nula ou <= 0 (DQ-15)", qtd_2, qtd_3)

mais_recente = Window.partitionBy("id_anuncio").orderBy(F.desc("data_coleta"))
etapa_4 = etapa_3.withColumn("_ordem", F.row_number().over(mais_recente)).filter("_ordem = 1").drop("_ordem")
qtd_4 = etapa_4.count()
registrar_impacto("Remove duplicados por id_anuncio (DQ-14)", qtd_3, qtd_4)

# COMMAND ----------

# MAGIC %md
# MAGIC Nenhum registro inválido nesta captura, como o diagnóstico indicava: os 91.067 anúncios seguem. Os filtros ficam no pipeline
# MAGIC como proteção para as próximas cargas da fonte.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Flags analíticas (sem remoção)
# MAGIC - `preco_valido`: diária preenchida e maior que zero (DQ-05).
# MAGIC - `outlier_preco`: fora dos limites de Tukey calculados sobre o **log** do preço, por cidade e tipo de quarto (DQ-06).
# MAGIC   O log é usado porque o preço é muito assimétrico; comparar dentro do mesmo tipo de quarto evita marcar como outlier uma casa
# MAGIC   inteira só por ser mais cara que um quarto compartilhado.
# MAGIC - `estadia_longa`: exige 30 noites ou mais — perfil de aluguel mensal (DQ-07).
# MAGIC - `anuncio_ativo`: recebeu ao menos uma avaliação nos 12 meses anteriores ao snapshot (DQ-07).

# COMMAND ----------

com_preco = etapa_4.withColumn("preco_valido", F.coalesce(F.col("preco_diaria") > 0, F.lit(False)))

limites_preco = (com_preco.filter("preco_valido")
                 .withColumn("log_preco", F.log(F.col("preco_diaria").cast("double")))
                 .groupBy("sigla_cidade", "tipo_quarto")
                 .agg(F.percentile_approx("log_preco", 0.25).alias("q1"),
                      F.percentile_approx("log_preco", 0.75).alias("q3"))
                 .withColumn("preco_limite_inferior", F.round(F.exp(F.col("q1") - 1.5 * (F.col("q3") - F.col("q1"))), 2))
                 .withColumn("preco_limite_superior", F.round(F.exp(F.col("q3") + 1.5 * (F.col("q3") - F.col("q1"))), 2))
                 .select("sigla_cidade", "tipo_quarto", "preco_limite_inferior", "preco_limite_superior"))
display(limites_preco.orderBy("sigla_cidade", "tipo_quarto"))

# COMMAND ----------

anuncios_silver = (com_preco
    .join(F.broadcast(limites_preco), ["sigla_cidade", "tipo_quarto"], "left")
    .withColumn("outlier_preco", F.coalesce(
        F.col("preco_valido") & ~F.col("preco_diaria").between(F.col("preco_limite_inferior"), F.col("preco_limite_superior")),
        F.lit(False)))
    .withColumn("estadia_longa", F.coalesce(F.col("noites_minimas") >= 30, F.lit(False)))
    .withColumn("anuncio_ativo", F.coalesce(F.col("num_avaliacoes_12m") > 0, F.lit(False)))
    .drop("preco_limite_inferior", "preco_limite_superior"))

display(anuncios_silver.groupBy("cidade").agg(
    F.count("*").alias("anuncios"),
    F.sum(F.col("preco_valido").cast("int")).alias("preco_valido"),
    F.sum(F.col("outlier_preco").cast("int")).alias("outlier_preco"),
    F.sum(F.col("estadia_longa").cast("int")).alias("estadia_longa"),
    F.sum(F.col("anuncio_ativo").cast("int")).alias("anuncio_ativo"),
).orderBy("cidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC Os limites de Tukey fazem sentido por tipo de quarto: uma casa inteira no RJ é considerada fora do padrão acima de ~R$ 3.200 por
# MAGIC noite, enquanto um quarto compartilhado já passa do limite acima de ~R$ 650. As flags marcam 2.377 outliers de preço em SP e
# MAGIC 1.516 no RJ, e mostram que 79% dos anúncios de SP e 68% dos do RJ tiveram atividade nos últimos 12 meses.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistência, validação e catálogo

# COMMAND ----------

colunas_finais = [
    "id_anuncio", "id_anfitriao", "sigla_cidade", "cidade", "bairro", "latitude", "longitude",
    "tipo_propriedade", "tipo_quarto", "capacidade_hospedes", "quartos", "camas", "banheiros", "qtd_comodidades",
    "preco_diaria", "noites_minimas", "noites_maximas", "dias_disponiveis_365",
    "noites_ocupadas_estimadas_12m", "receita_estimada_12m",
    "num_avaliacoes", "num_avaliacoes_12m", "avaliacoes_por_mes", "data_primeira_avaliacao", "data_ultima_avaliacao",
    "nota_geral", "nota_limpeza", "nota_localizacao", "nota_custo_beneficio",
    "meses_como_anfitriao", "anfitriao_superhost", "anfitriao_identidade_verificada", "qtd_anuncios_anfitriao_cidade",
    "preco_valido", "outlier_preco", "estadia_longa", "anuncio_ativo",
    "data_coleta", "data_snapshot",
]

destino = tabela_silver("anuncios")
salvar_tabela(anuncios_silver.select(*colunas_finais), destino)
validar_chave(destino, ["id_anuncio"])

sem_bairro_oficial = (spark.table(destino)
                      .join(spark.table(tabela_silver("bairros")), ["sigla_cidade", "bairro"], "left_anti").count())
print(f"Anúncios com bairro fora de silver.bairros (DQ-13): {sem_bairro_oficial:,}")

# COMMAND ----------

ORIGEM = "Origem: listings."
documentar_tabela(destino,
    "Silver | Anúncios do Airbnb em São Paulo e Rio de Janeiro, limpos, tipados e centralizados. "
    "Grão: 1 linha por anúncio (id_anuncio). Origem: bronze.listings_sp e bronze.listings_rj (união por nome). "
    "Registros inválidos removidos (sem id, fora do município, capacidade <= 0, duplicados). "
    "Flags preco_valido, outlier_preco, estadia_longa e anuncio_ativo indicam restrições para análise (tratamentos DQ-01 a DQ-15). "
    "Valores monetários em reais (BRL).",
    {
        "id_anuncio": f"Identificador do anúncio (chave primária). BIGINT convertido de texto para evitar perda de precisão (DQ-02). {ORIGEM}id",
        "id_anfitriao": f"Identificador do anfitrião. {ORIGEM}host_id",
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze.",
        "cidade": "Nome da cidade. Domínio: São Paulo, Rio de Janeiro.",
        "bairro": f"Bairro oficial, obtido pela fonte a partir da coordenada; chave para silver.bairros (DQ-04). {ORIGEM}neighbourhood_cleansed",
        "latitude": f"Latitude (graus decimais, anonimizada pela fonte em ~150 m). Validada contra o limite do município. {ORIGEM}latitude",
        "longitude": f"Longitude (graus decimais). Validada contra o limite do município. {ORIGEM}longitude",
        "tipo_propriedade": f"Tipo de propriedade declarado (ex.: Entire rental unit, Private room in home). {ORIGEM}property_type",
        "tipo_quarto": f"Tipo de acomodação. Domínio: Entire home/apt, Private room, Shared room, Hotel room. {ORIGEM}room_type",
        "capacidade_hospedes": f"Número máximo de hóspedes (> 0). {ORIGEM}accommodates",
        "quartos": f"Número de quartos (pode ser NULL). {ORIGEM}bedrooms",
        "camas": f"Número de camas (pode ser NULL). {ORIGEM}beds",
        "banheiros": f"Número de banheiros (pode ser NULL). {ORIGEM}bathrooms",
        "qtd_comodidades": f"Quantidade de comodidades listadas, contada a partir do array JSON. {ORIGEM}amenities",
        "preco_diaria": f"Preço da diária em BRL, convertido de texto como $1,250.00 (DQ-01). NULL quando ausente na fonte. {ORIGEM}price",
        "noites_minimas": f"Estadia mínima exigida, em noites. {ORIGEM}minimum_nights",
        "noites_maximas": f"Estadia máxima permitida, em noites. {ORIGEM}maximum_nights",
        "dias_disponiveis_365": f"Dias disponíveis para reserva nos próximos 365 dias (0 a 365). {ORIGEM}availability_365",
        "noites_ocupadas_estimadas_12m": f"Noites ocupadas estimadas pela fonte nos últimos 12 meses (modelo baseado em avaliações; 0 a 255 nesta versão). NULL se a versão da fonte não trouxer a coluna. {ORIGEM}estimated_occupancy_l365d",
        "receita_estimada_12m": f"Receita estimada pela fonte nos últimos 12 meses em BRL (diária × noites estimadas). {ORIGEM}estimated_revenue_l365d",
        "num_avaliacoes": f"Total de avaliações do anúncio. {ORIGEM}number_of_reviews",
        "num_avaliacoes_12m": f"Avaliações recebidas nos 12 meses anteriores ao snapshot. {ORIGEM}number_of_reviews_ltm",
        "avaliacoes_por_mes": f"Média de avaliações por mês desde a primeira avaliação. {ORIGEM}reviews_per_month",
        "data_primeira_avaliacao": f"Data da primeira avaliação (NULL se nunca avaliado). {ORIGEM}first_review",
        "data_ultima_avaliacao": f"Data da avaliação mais recente (NULL se nunca avaliado). {ORIGEM}last_review",
        "nota_geral": f"Nota geral média (0 a 5). NULL quando o anúncio nunca foi avaliado, sem imputação (DQ-08). {ORIGEM}review_scores_rating",
        "nota_limpeza": f"Nota média de limpeza (0 a 5). {ORIGEM}review_scores_cleanliness",
        "nota_localizacao": f"Nota média de localização (0 a 5). {ORIGEM}review_scores_location",
        "nota_custo_beneficio": f"Nota média de custo-benefício (0 a 5). {ORIGEM}review_scores_value",
        "meses_como_anfitriao": f"Tempo como anfitrião, em meses (anos x 12 + meses; 0 a ~190). Substitui host_since, vazio na fonte (DQ-16). {ORIGEM}hosts_time_as_host_years e hosts_time_as_host_months",
        "anfitriao_superhost": f"Anfitrião com selo Superhost (t/f convertido para boolean). {ORIGEM}host_is_superhost",
        "anfitriao_identidade_verificada": f"Identidade do anfitrião verificada (boolean). {ORIGEM}host_identity_verified",
        "qtd_anuncios_anfitriao_cidade": f"Anúncios do anfitrião no snapshot da cidade, calculado pela fonte (DQ-09). {ORIGEM}calculated_host_listings_count",
        "preco_valido": "Flag (DQ-05): true quando preco_diaria está preenchido e é maior que zero. Derivada.",
        "outlier_preco": "Flag (DQ-06): preço fora de exp(Q1 - 1,5*IQR) a exp(Q3 + 1,5*IQR) do log do preço, por cidade e tipo_quarto. Derivada.",
        "estadia_longa": "Flag (DQ-07): noites_minimas >= 30, perfil de aluguel mensal e não de temporada. Derivada.",
        "anuncio_ativo": "Flag (DQ-07): recebeu ao menos 1 avaliação nos 12 meses anteriores ao snapshot (num_avaliacoes_12m > 0). Derivada.",
        "data_coleta": f"Data em que a fonte coletou o anúncio. {ORIGEM}last_scraped",
        "data_snapshot": "Data do snapshot da cidade na fonte. Origem: metadado _data_snapshot da Bronze.",
    })

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {destino}"))
