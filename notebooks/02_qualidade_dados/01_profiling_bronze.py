# Databricks notebook source
# MAGIC %md
# MAGIC # Qualidade de Dados · Diagnóstico da Bronze
# MAGIC
# MAGIC Verificação feita **sobre os dados como foram capturados**, antes de qualquer transformação. Este notebook não grava tabelas:
# MAGIC ele mede os problemas e justifica as decisões da Silver. Cada problema recebe um código `DQ-xx`, citado depois nos notebooks
# MAGIC da Silver onde o tratamento é aplicado (rastreabilidade problema → solução).
# MAGIC
# MAGIC Dimensões avaliadas: **completude**, **consistência**, **unicidade**, **acurácia**, **outliers** e **integridade entre tabelas**.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

listings = ler_bronze_unificada("listings")
calendar = ler_bronze_unificada("calendar")
reviews = ler_bronze_unificada("reviews")
bairros = ler_bronze_unificada("neighbourhoods")

def colunas_fonte(df):
    return [c for c in df.columns if not c.startswith("_")]   # ignora os metadados de ingestão

display(spark.createDataFrame(
    [(tabela_bronze(e, s), spark.table(tabela_bronze(e, s)).count()) for e in ENTIDADES for s in CIDADES],
    "tabela string, linhas long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Consistência de schema entre as cidades
# MAGIC Os snapshots de SP e RJ foram coletados em datas diferentes; é preciso confirmar se as colunas coincidem antes de unir as bases.

# COMMAND ----------

for entidade in ENTIDADES:
    colunas_sp = set(colunas_fonte(spark.table(tabela_bronze(entidade, "sp"))))
    colunas_rj = set(colunas_fonte(spark.table(tabela_bronze(entidade, "rj"))))
    print(f"{entidade:<15} colunas SP={len(colunas_sp)} RJ={len(colunas_rj)} | "
          f"só SP: {sorted(colunas_sp - colunas_rj) or '-'} | só RJ: {sorted(colunas_rj - colunas_sp) or '-'}")

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-03** — Nesta captura as duas cidades têm exatamente as mesmas colunas (90 em listings, 5 em calendar, 6 em reviews).
# MAGIC Como a fonte muda o layout entre versões (ver DQ-10 e DQ-16), a centralização na Silver usa `unionByName(allowMissingColumns=True)`
# MAGIC e **seleção explícita** das colunas usadas, protegendo o pipeline de mudanças futuras.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Listings (anúncios)
# MAGIC ### 2.1 Completude de todas as colunas

# COMMAND ----------

display(perfil_completude(listings, colunas_fonte(listings)))

# COMMAND ----------

# MAGIC %md
# MAGIC Três grupos aparecem na completude:
# MAGIC - **Colunas 100% vazias** (12): `host_since`, `host_response_time`, `host_response_rate`, `host_acceptance_rate`,
# MAGIC   `host_total_listings_count`, `host_neighbourhood`, `host_thumbnail_url`, `neighbourhood`, `neighborhood_overview`,
# MAGIC   `instant_bookable`, `license` e `calendar_updated`. A fonte deixou de publicar esses dados nesta versão.
# MAGIC - **Vazios parciais com explicação a investigar**: notas e datas de avaliação (17,1%), `price` e `estimated_revenue_l365d` (5,2%)
# MAGIC   e `neighbourhood_group_cleansed` (53,5%).
# MAGIC - **Atributos físicos com vazios moderados**: `bedrooms` (14,0%), `bathrooms` (10,9%) e `beds` (6,4%).
# MAGIC
# MAGIC **DQ-16** — Colunas totalmente vazias não carregam informação. **Tratamento:** ficam fora da Silver. O tempo de casa do
# MAGIC anfitrião, que viria de `host_since`, é obtido das colunas que a fonte passou a publicar (`hosts_time_as_host_years` e
# MAGIC `hosts_time_as_host_months`, completas).
# MAGIC
# MAGIC ### 2.2 Bairro e agrupamento de bairros

# COMMAND ----------

display(listings.groupBy("_cidade").agg(
    F.countDistinct("neighbourhood_cleansed").alias("bairros_distintos"),
    F.countDistinct("neighbourhood_group_cleansed").alias("grupos_distintos"),
    F.sum(F.when(F.col("neighbourhood_group_cleansed").isNull(), 1).otherwise(0)).alias("anuncios_sem_grupo"),
).orderBy("_cidade"))
display(listings.filter(F.col("_cidade") == "sp")
        .groupBy("neighbourhood_group_cleansed").count()
        .orderBy(F.desc("count"), "neighbourhood_group_cleansed").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-04** — `neighbourhood` (texto livre do anfitrião) está vazio; o bairro vem de `neighbourhood_cleansed`, que a fonte obtém
# MAGIC pela coordenada do imóvel e está completo (96 bairros usados em SP e 154 dos 160 no RJ). O agrupamento
# MAGIC `neighbourhood_group_cleansed` só existe em **SP**, onde corresponde às 32 subprefeituras; no RJ está vazio.
# MAGIC **Tratamento:** `neighbourhood_cleansed` é o bairro; `neighbourhood` é descartado; o grupo fica apenas em `silver.bairros` como
# MAGIC atributo opcional (não serve para comparar as duas cidades).
# MAGIC
# MAGIC ### 2.3 Unicidade e formato dos identificadores

# COMMAND ----------

print("IDs de anúncio duplicados:", contar_duplicados(listings, ["id"]))

ids = listings.select("id").withColumn("digitos", F.length("id"))
display(ids.groupBy("digitos").count().orderBy("digitos"))

perda_precisao = ids.filter(F.expr("try_cast(id AS BIGINT) <> try_cast(try_cast(id AS DOUBLE) AS BIGINT)")).count()
print(f"IDs que mudariam de valor se fossem lidos como DOUBLE: {perda_precisao:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-02** — Convivem dois padrões de ID: anúncios antigos com 4 a 8 dígitos e anúncios recentes com 18 a 19 dígitos (~80% da base).
# MAGIC Os longos passam da precisão exata de um `double` (2^53): lidos como número decimal, 73.058 IDs seriam arredondados e os JOINs com
# MAGIC `calendar` e `reviews` quebrariam. **Tratamento:** Bronze em STRING e conversão direta para `BIGINT` na Silver.
# MAGIC
# MAGIC **DQ-14** — Não há `id` duplicado nesta captura. Ainda assim a Silver aplica deduplicação defensiva e valida a chave após a carga.
# MAGIC
# MAGIC ### 2.4 Consistência de formatos

# COMMAND ----------

display(listings.select(
    F.sum(F.when(F.col("price").isNotNull() & ~F.col("price").rlike(r"^\$[0-9,]+\.[0-9]{2}$"), 1).otherwise(0)).alias("price_fora_do_padrao"),
    F.sum(F.when(F.col("host_is_superhost").isNotNull() & ~F.col("host_is_superhost").isin("t", "f"), 1).otherwise(0)).alias("superhost_fora_de_t_f"),
    F.sum(F.when(F.col("host_identity_verified").isNotNull() & ~F.col("host_identity_verified").isin("t", "f"), 1).otherwise(0)).alias("identidade_fora_de_t_f"),
    F.sum(F.when(~F.col("last_scraped").rlike(r"^\d{4}-\d{2}-\d{2}$"), 1).otherwise(0)).alias("data_coleta_fora_do_iso"),
    F.sum(F.when(~F.col("amenities").startswith("["), 1).otherwise(0)).alias("amenities_fora_de_lista_json"),
))
display(listings.select("id", "price", "host_is_superhost", "host_identity_verified", "last_scraped", "bathrooms_text",
                        F.substring("amenities", 1, 60).alias("amenities_inicio"))
        .orderBy("id").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-01** — Os formatos são consistentes (nenhum valor fora do padrão), mas nenhum está no tipo correto: preço como texto com
# MAGIC `$` e vírgula de milhar (o valor está em **reais**, apesar do símbolo), sim/não como `t`/`f`, datas como texto ISO e
# MAGIC comodidades como uma lista JSON dentro de um texto. **Tratamento:** conversão na Silver com `try_cast`, com contagem de falhas
# MAGIC de conversão (devem ser zero).
# MAGIC
# MAGIC ### 2.5 Período de coleta

# COMMAND ----------

display(listings.groupBy("_cidade", "_data_snapshot").agg(
    F.min("last_scraped").alias("primeira_coleta"),
    F.max("last_scraped").alias("ultima_coleta"),
).orderBy("_cidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-17** — A data do snapshot (usada no nome da pasta da fonte) é o **início** da coleta, que dura alguns dias: em SP vai de
# MAGIC 14 a 16/06 e no RJ de 25/06 a 01/07 (um dia depois da data do snapshot, 24/06). Qualquer regra de data que use o snapshot como
# MAGIC limite descartaria registros válidos. **Tratamento:** a Silver usa a **última data de coleta** de cada cidade como limite (ver 4).
# MAGIC
# MAGIC ### 2.6 Acurácia e valores extremos

# COMMAND ----------

numericos = listings.select(
    "_cidade", "room_type",
    para_double("latitude").alias("lat"), para_double("longitude").alias("lon"),
    para_inteiro("accommodates").alias("hospedes"),
    para_inteiro("minimum_nights").alias("noites_min"),
    para_inteiro("number_of_reviews_ltm").alias("avaliacoes_12m"),
    para_inteiro("availability_365").alias("dias_disponiveis_365"),
    parse_preco("price").alias("preco"),
)
display(numericos.groupBy("_cidade").agg(
    F.min("lat").alias("lat_min"), F.max("lat").alias("lat_max"),
    F.min("lon").alias("lon_min"), F.max("lon").alias("lon_max"),
    F.min("hospedes").alias("hospedes_min"), F.max("hospedes").alias("hospedes_max"),
    F.max("noites_min").alias("noites_minimas_max"),
    F.sum(F.when(F.col("noites_min") >= 30, 1).otherwise(0)).alias("exigem_30_noites_ou_mais"),
    F.sum(F.when(F.col("avaliacoes_12m") == 0, 1).otherwise(0)).alias("sem_avaliacao_12m"),
).orderBy("_cidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-15** — As coordenadas ficam dentro dos limites de cada município e a capacidade vai de 1 a 16 hóspedes: não há erro grosseiro
# MAGIC de acurácia. A Silver mantém essas regras como filtro de validação (registro inválido é removido e contado).
# MAGIC
# MAGIC **DQ-07** — 668 anúncios em SP e 536 no RJ exigem **30 noites ou mais** (na prática, aluguel mensal, não temporada), com mínimo de
# MAGIC até 730 noites no RJ. E 8.707 anúncios em SP (21%) e 15.646 no RJ (32%) **não receberam nenhuma avaliação em 12 meses** —
# MAGIC provavelmente inativos. Ambos distorcem análises de aluguel de curta duração. **Tratamento:** flags `estadia_longa` e
# MAGIC `anuncio_ativo` na Silver; o filtro é decidido na Gold.

# COMMAND ----------

display(numericos.groupBy("_cidade").agg(
    F.count("preco").alias("anuncios_com_preco"),
    F.sum(F.when(F.col("preco").isNull(), 1).otherwise(0)).alias("anuncios_sem_preco"),
    *[F.percentile_approx("preco", p).alias(f"p{int(p * 100):02d}") for p in (0.01, 0.25, 0.50, 0.75, 0.99)],
    F.min("preco").alias("preco_min"),
    F.max("preco").alias("preco_max"),
).orderBy("_cidade"))

# COMMAND ----------

# Os preços mais altos: são diárias reais?
display(numericos.filter("preco IS NOT NULL")
        .select("_cidade", "room_type", "hospedes", "preco", "avaliacoes_12m", "dias_disponiveis_365")
        .orderBy(F.desc("preco"), "_cidade").limit(10))

# COMMAND ----------

# Anúncios sem preço: o que eles têm em comum?
display(numericos.groupBy("_cidade", F.col("preco").isNull().alias("sem_preco")).agg(
    F.count("*").alias("anuncios"),
    F.round(F.avg("dias_disponiveis_365"), 1).alias("media_dias_disponiveis_365"),
    F.round(100 * F.avg((F.col("avaliacoes_12m") == 0).cast("int")), 1).alias("pct_sem_avaliacao_12m"),
).orderBy("_cidade", "sem_preco"))

# COMMAND ----------

# Distribuição do preço em escala log (usar a visualização Histogram, agrupada por _cidade)
display(numericos.filter("preco > 0").select("_cidade", "room_type", F.log10("preco").alias("log10_preco")))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-06** — O preço tem cauda longa: a mediana é R$ 331 em SP e R$ 453 no RJ, o p99 fica em ~R$ 1.900 e ~R$ 7.560, mas o
# MAGIC máximo chega a R$ 57.127 em SP e **R$ 574.013** no RJ. Dos 10 maiores valores, 9 são de anúncios sem nenhuma avaliação em
# MAGIC 12 meses, típico de preço "de bloqueio" e não de diária real. No outro extremo há diárias de R$ 5,54. Médias ficariam distorcidas.
# MAGIC **Tratamento:** flag `outlier_preco` pela regra de Tukey (1,5 × IQR) aplicada ao **log do preço**, por cidade e tipo de quarto;
# MAGIC a análise usará medianas e excluirá os outliers.
# MAGIC
# MAGIC **DQ-05** — 522 anúncios em SP (1,2%) e 4.171 no RJ (8,6%) não têm preço. Nas duas cidades, mais da metade deles (~56%) não
# MAGIC teve avaliação em 12 meses, contra 20–30% dos demais; no RJ, eles ainda têm em média só 33 dias disponíveis no ano (contra 213):
# MAGIC são, em sua maioria, anúncios parados ou com agenda fechada. Não é possível estimar a diária sem inventar dado. **Tratamento:** manter o anúncio (ele ainda conta para oferta e perfil de anfitrião) com `preco_valido = false`.
# MAGIC
# MAGIC ### 2.7 Notas vazias: erro ou ausência estrutural?

# COMMAND ----------

display(listings.groupBy((para_inteiro("number_of_reviews") == 0).alias("anuncio_sem_avaliacoes")).agg(
    F.count("*").alias("anuncios"),
    F.sum(F.when(F.col("review_scores_rating").isNull(), 1).otherwise(0)).alias("nota_geral_vazia"),
).orderBy("anuncio_sem_avaliacoes"))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-08** — Os 15.594 anúncios com nota vazia são exatamente os que **nunca foram avaliados**: é ausência estrutural, não falha de
# MAGIC coleta. **Tratamento:** manter NULL (imputar uma nota inventaria reputação que o anúncio não tem).
# MAGIC
# MAGIC ### 2.8 Quantidade de anúncios por anfitrião

# COMMAND ----------

anfitrioes = listings.select(
    "host_id", "_cidade",
    para_inteiro("host_listings_count").alias("qtd_perfil"),
    para_inteiro("calculated_host_listings_count").alias("qtd_calculada"),
)
display(anfitrioes.agg(
    F.count("*").alias("anuncios"),
    F.sum(F.when(F.col("qtd_perfil") != F.col("qtd_calculada"), 1).otherwise(0)).alias("contagens_divergentes"),
    F.sum(F.when(F.col("qtd_perfil").isNull(), 1).otherwise(0)).alias("qtd_perfil_vazia"),
))
em_duas_cidades = anfitrioes.groupBy("host_id").agg(F.countDistinct("_cidade").alias("n")).filter("n > 1").count()
print(f"Anfitriões com anúncios nas duas cidades: {em_duas_cidades:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-09** — `host_listings_count` vem do perfil do anfitrião na plataforma e diverge de `calculated_host_listings_count` (calculado
# MAGIC pela fonte com os anúncios presentes no snapshot) em 38.009 anúncios (42%). Além disso, 192 anfitriões atuam nas duas cidades.
# MAGIC **Tratamento:** usar a contagem calculada na Silver e recalcular o portfólio na base unificada SP + RJ na Gold (pergunta 2).
# MAGIC
# MAGIC ## 3. Calendar (disponibilidade diária)

# COMMAND ----------

print("Colunas do calendário:", colunas_fonte(calendar))
display(calendar.groupBy("_cidade").agg(
    F.count("*").alias("linhas"),
    F.countDistinct("listing_id").alias("anuncios"),
    F.min("date").alias("data_inicial"),
    F.max("date").alias("data_final"),
).orderBy("_cidade"))
display(calendar.groupBy("_cidade", "listing_id").count()
        .groupBy("_cidade", F.col("count").alias("dias_por_anuncio")).agg(F.count("*").alias("anuncios"))
        .orderBy("_cidade", F.desc("anuncios"), "dias_por_anuncio"))

# COMMAND ----------

print("Duplicados (listing_id, date):", contar_duplicados(calendar, ["listing_id", "date"]))
display(perfil_completude(calendar, colunas_fonte(calendar)))
display(calendar.groupBy("_cidade", "available").count().orderBy("_cidade", "available"))

# COMMAND ----------

# MAGIC %md
# MAGIC O calendário está completo e sem duplicados: 365 dias por anúncio (3 anúncios de SP com 366), sem nulos, com `available` sempre `t`/`f`.
# MAGIC
# MAGIC **DQ-10** — O calendário desta versão da fonte **não tem colunas de preço** (versões antigas traziam `price` e `adjusted_price`):
# MAGIC ele informa apenas se cada dia está disponível. **Tratamento:** a diária vem de `listings.price`; a Silver do calendário verifica a
# MAGIC existência da coluna antes de usá-la, para não quebrar caso a fonte volte a publicá-la.
# MAGIC
# MAGIC **DQ-11** — O calendário cobre os **365 dias seguintes** à coleta (é uma visão futura, não histórica), e `available = f` não
# MAGIC distingue *reservado* de *bloqueado pelo anfitrião*. Uma ocupação calculada só com ele tende a ser superestimada.
# MAGIC **Tratamento:** limitação documentada; a Gold usa `estimated_occupancy_l365d` (estimativa da própria fonte para os últimos 12
# MAGIC meses, completa em 100% dos anúncios) como métrica principal e o calendário como indicador complementar.
# MAGIC
# MAGIC ## 4. Reviews (avaliações)

# COMMAND ----------

display(perfil_completude(reviews, colunas_fonte(reviews)))
display(reviews.groupBy("_cidade", "_data_snapshot").agg(
    F.count("*").alias("avaliacoes"),
    F.countDistinct("listing_id").alias("anuncios_avaliados"),
    F.min("date").alias("primeira"),
    F.max("date").alias("ultima"),
    F.sum(F.when(F.col("date") > F.col("_data_snapshot").cast("string"), 1).otherwise(0)).alias("depois_da_data_do_snapshot"),
).orderBy("_cidade"))
print("IDs de avaliação duplicados:", contar_duplicados(reviews, ["id"]))

# COMMAND ----------

qtd_no_arquivo = reviews.groupBy("listing_id").agg(F.count("*").alias("qtd_no_arquivo_reviews"))
comparacao = (listings.select(F.col("id").alias("listing_id"), para_inteiro("number_of_reviews").alias("qtd_no_cadastro"))
              .join(qtd_no_arquivo, "listing_id", "left")
              .fillna(0, ["qtd_no_arquivo_reviews"]))
display(comparacao.agg(
    F.count("*").alias("anuncios"),
    F.sum(F.when(F.col("qtd_no_cadastro") == F.col("qtd_no_arquivo_reviews"), 1).otherwise(0)).alias("contagem_igual"),
    F.sum(F.when(F.col("qtd_no_cadastro") != F.col("qtd_no_arquivo_reviews"), 1).otherwise(0)).alias("contagem_diferente"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC As avaliações começam em 2010 (RJ) e 2011 (SP), não têm IDs duplicados e a contagem por anúncio bate 100% com
# MAGIC `number_of_reviews` do cadastro: as duas tabelas são consistentes entre si. Há avaliações com data **posterior à data do
# MAGIC snapshot** (192 em SP e 107 no RJ), mas nenhuma posterior ao fim da coleta — confirma o DQ-17.
# MAGIC
# MAGIC **DQ-12** — `reviewer_name` e `comments` são dados pessoais e texto livre sem uso nas perguntas.
# MAGIC **Tratamento:** descartados na Silver (minimização de dados, em linha com a LGPD); `reviewer_id` também não é necessário.
# MAGIC
# MAGIC ## 5. Integridade entre tabelas

# COMMAND ----------

ids_anuncios = listings.select(F.col("id").alias("listing_id")).distinct()
bairros_oficiais = bairros.select("_cidade", "neighbourhood")
bairros_nos_anuncios = listings.select("_cidade", F.col("neighbourhood_cleansed").alias("neighbourhood")).distinct()

display(spark.createDataFrame([
    ("anúncios do calendar ausentes em listings", calendar.select("listing_id").distinct().join(ids_anuncios, "listing_id", "left_anti").count()),
    ("anúncios de reviews ausentes em listings", reviews.select("listing_id").distinct().join(ids_anuncios, "listing_id", "left_anti").count()),
    ("bairros de listings fora da lista oficial", bairros_nos_anuncios.join(bairros_oficiais, ["_cidade", "neighbourhood"], "left_anti").count()),
], "verificacao string, ocorrencias long"))

# COMMAND ----------

# MAGIC %md
# MAGIC **DQ-13** — Todo bairro usado nos anúncios existe na lista oficial. Mas **40 anúncios** aparecem no calendário e **35** nas
# MAGIC avaliações sem existir em `listings` (provavelmente removidos entre a coleta dos arquivos). São registros órfãos: não têm preço,
# MAGIC bairro nem anfitrião. **Tratamento:** a Silver aplica a integridade com semi-join em `silver.anuncios` e registra quantas
# MAGIC linhas foram removidas.
# MAGIC
# MAGIC ## 6. Resumo: problemas e tratamentos
# MAGIC
# MAGIC | Código | Problema | Tratamento | Onde |
# MAGIC |---|---|---|---|
# MAGIC | DQ-01 | Tipos em texto: preço `$1,250.00` (em BRL), `t`/`f`, datas, comodidades em JSON | Conversão com `try_cast` + contagem de falhas | Silver |
# MAGIC | DQ-02 | IDs de 18–19 dígitos perdem precisão como `double` (73.058 casos) | STRING na Bronze → `BIGINT` na Silver | Bronze / Silver |
# MAGIC | DQ-03 | Layout da fonte muda entre versões | `unionByName` + seleção explícita de colunas | Silver |
# MAGIC | DQ-04 | `neighbourhood` vazio; agrupamento de bairros só em SP | Usar `neighbourhood_cleansed`; grupo só como atributo de `silver.bairros` | Silver |
# MAGIC | DQ-05 | Anúncios sem preço (1,2% SP; 8,6% RJ), em geral com agenda fechada | Manter com flag `preco_valido` | Silver (flag) / Gold (filtro) |
# MAGIC | DQ-06 | Outliers de preço (até R$ 574 mil/noite; mínimos de R$ 5) | Flag `outlier_preco` (Tukey no log, cidade × tipo de quarto) | Silver (flag) / Gold (filtro) |
# MAGIC | DQ-07 | Estadia mínima ≥ 30 noites; anúncios sem avaliação em 12 meses | Flags `estadia_longa` e `anuncio_ativo` | Silver (flag) / Gold (filtro) |
# MAGIC | DQ-08 | Notas vazias em anúncios nunca avaliados | Manter NULL, sem imputação | Silver |
# MAGIC | DQ-09 | Contagem de anúncios por anfitrião divergente (42%); 192 anfitriões nas 2 cidades | Contagem calculada + recálculo na base unificada | Silver / Gold |
# MAGIC | DQ-10 | Calendário sem colunas de preço nesta versão | Diária vem de `listings`; Silver checa a existência da coluna | Silver |
# MAGIC | DQ-11 | Calendário é futuro e `f` é ambíguo (reservado × bloqueado) | Ocupação estimada da fonte como métrica principal | Gold / Análise |
# MAGIC | DQ-12 | Dados pessoais em reviews | Colunas removidas | Silver |
# MAGIC | DQ-13 | 40 anúncios órfãos no calendário e 35 nas avaliações | Semi-join com `silver.anuncios` | Silver |
# MAGIC | DQ-14 | Risco de chaves duplicadas (nenhuma encontrada) | Deduplicação + validação pós-carga | Silver |
# MAGIC | DQ-15 | Acurácia de coordenadas, capacidade e datas | Filtro de registros inválidos | Silver |
# MAGIC | DQ-16 | 12 colunas 100% vazias (ex.: `host_since`, `host_response_rate`, `instant_bookable`) | Excluídas; tempo de anfitrião via `hosts_time_as_host_*` | Silver |
# MAGIC | DQ-17 | A coleta dura dias; datas passam da data do snapshot | Limite de datas = última data de coleta da cidade | Silver |
