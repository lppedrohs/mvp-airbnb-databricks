# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · fato_anuncio
# MAGIC
# MAGIC Tabela fato central do modelo: **1 linha por anúncio**, com as medidas usadas nas três perguntas e as chaves para todas as dimensões.
# MAGIC
# MAGIC | Bloco | Conteúdo | Origem |
# MAGIC |---|---|---|
# MAGIC | Chaves | `id_anuncio` (→ dim_imovel), `id_anfitriao` (→ dim_anfitriao), `sk_bairro` (→ dim_bairro) | silver.anuncios + dim_bairro |
# MAGIC | Preço e rentabilidade | diária, noites ocupadas, taxa de ocupação e receita anual estimadas | silver.anuncios (recalculado) |
# MAGIC | Disponibilidade futura | % de dias reservados ou bloqueados nos próximos 12 meses | **fato_disponibilidade_mensal** (agregado) |
# MAGIC | Demanda e reputação | avaliações totais e nos últimos 365 dias, notas | silver.anuncios + **fato_avaliacao** (agregado) |
# MAGIC | Elegibilidade | flags da Silver + `elegivel_analise` | silver.anuncios |
# MAGIC
# MAGIC **Rentabilidade = receita bruta anual estimada** (diária × noites ocupadas estimadas pela fonte). A base não traz o valor de compra
# MAGIC dos imóveis, então não é possível calcular retorno sobre o investimento. A ocupação estimada pela fonte tem teto de 255 noites.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

def tabela_gold(nome):
    return f"{SCHEMA_GOLD}.{nome}"


anuncios = spark.table(tabela_silver("anuncios"))
dim_bairro = spark.table(tabela_gold("dim_bairro")).select("sigla_cidade", "bairro", "sk_bairro")

# Resumo do calendário (próximos 12 meses) a partir de outra tabela da Gold
resumo_calendario = (spark.table(tabela_gold("fato_disponibilidade_mensal"))
                     .groupBy("id_anuncio")
                     .agg(F.round(F.sum("dias_indisponiveis") / F.sum("dias_no_calendario"), 4)
                          .alias("pct_dias_indisponiveis_futuro")))

# Avaliações dos 365 dias anteriores à coleta de cada anúncio, a partir da fato_avaliacao
resumo_avaliacoes = (spark.table(tabela_gold("fato_avaliacao"))
                     .join(anuncios.select("id_anuncio", "data_coleta"), "id_anuncio")
                     .filter(F.col("data_avaliacao") >= F.date_sub("data_coleta", 365))
                     .groupBy("id_anuncio")
                     .agg(F.count("*").alias("avaliacoes_365d")))

# COMMAND ----------

fato_anuncio = (anuncios
    .join(dim_bairro, ["sigla_cidade", "bairro"], "left")
    .join(resumo_calendario, "id_anuncio", "left")
    .join(resumo_avaliacoes, "id_anuncio", "left")
    .select(
        "id_anuncio", "id_anfitriao", "sk_bairro", "sigla_cidade",
        "preco_diaria",
        "noites_ocupadas_estimadas_12m",
        F.round(F.col("noites_ocupadas_estimadas_12m") / 365, 4).alias("taxa_ocupacao_estimada"),
        (F.col("preco_diaria") * F.col("noites_ocupadas_estimadas_12m")).cast("decimal(14,2)").alias("receita_estimada_12m"),
        "dias_disponiveis_365",
        "pct_dias_indisponiveis_futuro",
        "num_avaliacoes",
        F.coalesce("avaliacoes_365d", F.lit(0)).alias("avaliacoes_365d"),
        "nota_geral", "nota_limpeza", "nota_localizacao", "nota_custo_beneficio",
        "preco_valido", "outlier_preco", "estadia_longa", "anuncio_ativo",
        (F.col("preco_valido") & ~F.col("outlier_preco") & ~F.col("estadia_longa") & F.col("anuncio_ativo"))
            .alias("elegivel_analise"),
        "data_snapshot"))

destino = tabela_gold("fato_anuncio")
salvar_tabela(fato_anuncio, destino)
validar_chave(destino, ["id_anuncio"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validações do modelo
# MAGIC Toda linha da fato deve encontrar sua dimensão, e as avaliações dos últimos 365 dias calculadas na Gold devem bater com a contagem
# MAGIC da fonte (`number_of_reviews_ltm`). A janela que reproduz a fonte inclui o próprio dia de 365 dias antes da coleta (`>=`); com
# MAGIC `>` apareciam 2.548 diferenças. Com a janela correta, a diferença residual é de 1 anúncio em 91.067.

# COMMAND ----------

fato = spark.table(destino)
display(spark.createDataFrame([
    ("anúncios sem bairro em dim_bairro", fato.filter("sk_bairro IS NULL").count()),
    ("anúncios sem anfitrião em dim_anfitriao", fato.join(spark.table(tabela_gold("dim_anfitriao")), "id_anfitriao", "left_anti").count()),
    ("anúncios sem imóvel em dim_imovel", fato.join(spark.table(tabela_gold("dim_imovel")), "id_anuncio", "left_anti").count()),
    ("anúncios sem calendário", fato.filter("pct_dias_indisponiveis_futuro IS NULL").count()),
    ("avaliacoes_365d diferente da fonte", fato.join(anuncios.select("id_anuncio", "num_avaliacoes_12m"), "id_anuncio")
                                               .filter("avaliacoes_365d <> num_avaliacoes_12m").count()),
], "verificacao string, ocorrencias long"))

display(fato.groupBy("sigla_cidade").agg(
    F.count("*").alias("anuncios"),
    F.sum(F.col("elegivel_analise").cast("int")).alias("elegiveis_analise"),
    F.expr("percentile_approx(CASE WHEN elegivel_analise THEN preco_diaria END, 0.5)").alias("mediana_diaria_elegiveis"),
    F.expr("percentile_approx(CASE WHEN elegivel_analise THEN noites_ocupadas_estimadas_12m END, 0.5)").alias("mediana_noites_elegiveis"),
    F.expr("percentile_approx(CASE WHEN elegivel_analise THEN receita_estimada_12m END, 0.5)").alias("mediana_receita_elegiveis"),
).orderBy("sigla_cidade"))

# COMMAND ----------

documentar_tabela(destino,
    "Gold | Fato central: 1 linha por anúncio com preço, ocupação e receita anual estimadas, disponibilidade futura, demanda e notas. "
    "Origem: silver.anuncios + dim_bairro + resumos de fato_disponibilidade_mensal e fato_avaliacao. "
    "Rentabilidade = receita bruta anual estimada (BRL); a base não traz o valor do imóvel.",
    {
        "id_anuncio": "Identificador do anúncio (chave); chave estrangeira para dim_imovel.",
        "id_anfitriao": "Anfitrião; chave estrangeira para dim_anfitriao.",
        "sk_bairro": "Bairro; chave estrangeira para dim_bairro.",
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj.",
        "preco_diaria": "Preço da diária em BRL (NULL quando a fonte não informa, DQ-05). Origem: silver.anuncios.",
        "noites_ocupadas_estimadas_12m": "Noites ocupadas nos últimos 12 meses estimadas pela fonte (0 a 255). Origem: silver.anuncios.",
        "taxa_ocupacao_estimada": "noites_ocupadas_estimadas_12m / 365 (0 a ~0,70). Derivada.",
        "receita_estimada_12m": "Receita bruta anual estimada em BRL = preco_diaria x noites_ocupadas_estimadas_12m (igual à estimativa da fonte). Derivada.",
        "dias_disponiveis_365": "Dias livres nos próximos 365 dias segundo a fonte (0 a 365). Origem: silver.anuncios.",
        "pct_dias_indisponiveis_futuro": "Parcela dos próximos 12 meses reservada ou bloqueada (0 a 1; DQ-11). Derivada de fato_disponibilidade_mensal.",
        "num_avaliacoes": "Total de avaliações do anúncio. Origem: silver.anuncios.",
        "avaliacoes_365d": "Avaliações nos 365 dias anteriores à coleta do anúncio. Derivada de fato_avaliacao.",
        "nota_geral": "Nota geral (0 a 5; NULL se nunca avaliado, DQ-08). Origem: silver.anuncios.",
        "nota_limpeza": "Nota de limpeza (0 a 5). Origem: silver.anuncios.",
        "nota_localizacao": "Nota de localização (0 a 5). Origem: silver.anuncios.",
        "nota_custo_beneficio": "Nota de custo-benefício (0 a 5). Origem: silver.anuncios.",
        "preco_valido": "Flag DQ-05 (preço preenchido e > 0). Origem: silver.anuncios.",
        "outlier_preco": "Flag DQ-06 (fora dos limites de Tukey no log do preço). Origem: silver.anuncios.",
        "estadia_longa": "Flag DQ-07 (estadia mínima >= 30 noites). Origem: silver.anuncios.",
        "anuncio_ativo": "Flag DQ-07 (ao menos 1 avaliação em 12 meses). Origem: silver.anuncios.",
        "elegivel_analise": "true quando preco_valido e anuncio_ativo, sem outlier_preco e sem estadia_longa: base das análises de preço e rentabilidade. Derivada.",
        "data_snapshot": "Data do snapshot da cidade na fonte.",
    })
display(spark.sql(f"DESCRIBE TABLE {destino}"))
