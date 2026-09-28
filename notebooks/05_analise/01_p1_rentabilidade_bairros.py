# Databricks notebook source
# MAGIC %md
# MAGIC # P1 · Quais bairros geram maior retorno para o anfitrião?
# MAGIC
# MAGIC > Quais bairros geram maior retorno para um anfitrião, considerando não só o preço da diária, mas a ocupação ao longo do ano?
# MAGIC
# MAGIC **Métrica:** receita bruta anual estimada = diária × noites ocupadas estimadas nos últimos 12 meses (`gold.fato_anuncio`).
# MAGIC **Base:** anúncios elegíveis (preço válido, sem outlier, estadia mínima < 30 noites e ativos em 12 meses).
# MAGIC **Regra:** só entram no ranking bairros com pelo menos 30 anúncios elegíveis. Usamos **medianas**, que não são distorcidas por
# MAGIC valores extremos.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

MIN_ANUNCIOS_BAIRRO = 30

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW p1_bairros AS
SELECT b.cidade,
       b.bairro,
       COUNT(*)                                            AS anuncios_elegiveis,
       percentile_approx(f.preco_diaria, 0.5)              AS mediana_diaria,
       percentile_approx(f.noites_ocupadas_estimadas_12m, 0.5) AS mediana_noites_ocupadas,
       percentile_approx(f.receita_estimada_12m, 0.5)      AS mediana_receita_anual
FROM {SCHEMA_GOLD}.fato_anuncio f
JOIN {SCHEMA_GOLD}.dim_bairro b ON f.sk_bairro = b.sk_bairro
WHERE f.elegivel_analise
GROUP BY b.cidade, b.bairro
HAVING COUNT(*) >= {MIN_ANUNCIOS_BAIRRO}
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW p1_ranking AS
SELECT *,
       RANK() OVER (PARTITION BY cidade ORDER BY mediana_receita_anual DESC, bairro) AS posicao_receita,
       RANK() OVER (PARTITION BY cidade ORDER BY mediana_diaria DESC, bairro)        AS posicao_diaria
FROM p1_bairros
""")

display(spark.sql("""
SELECT cidade, COUNT(*) AS bairros_no_ranking, SUM(anuncios_elegiveis) AS anuncios_cobertos
FROM p1_ranking GROUP BY cidade ORDER BY cidade
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 10 bairros por receita anual (mediana), com a posição que teriam pelo preço da diária

# COMMAND ----------

display(spark.sql("""
SELECT cidade, posicao_receita, bairro, anuncios_elegiveis,
       mediana_receita_anual, mediana_diaria, mediana_noites_ocupadas, posicao_diaria
FROM p1_ranking
WHERE posicao_receita <= 10
ORDER BY cidade, posicao_receita
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## O ranking por diária é um bom substituto do ranking por receita?
# MAGIC A correlação entre as duas posições (correlação de Spearman) mede o quanto os bairros mais caros coincidem com os que mais rendem:
# MAGIC 1 = rankings idênticos; perto de 0 = sem relação.

# COMMAND ----------

display(spark.sql("""
SELECT cidade,
       ROUND(corr(posicao_receita, posicao_diaria), 2)                            AS correlacao_ranking_diaria_x_receita,
       ROUND(corr(mediana_noites_ocupadas, mediana_diaria), 2)                    AS correlacao_ocupacao_x_diaria,
       SUM(CASE WHEN posicao_diaria <= 10 AND posicao_receita > 10 THEN 1 ELSE 0 END) AS top10_diaria_fora_do_top10_receita
FROM p1_ranking GROUP BY cidade ORDER BY cidade
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bairros caros que rendem menos do que o preço sugere
# MAGIC Bairros no top 10 de diária que ficam fora do top 10 de receita: o preço alto não se converte em receita por causa da ocupação.

# COMMAND ----------

display(spark.sql("""
SELECT cidade, bairro, posicao_diaria, posicao_receita, mediana_diaria, mediana_noites_ocupadas, mediana_receita_anual
FROM p1_ranking
WHERE posicao_diaria <= 10 AND posicao_receita > 10
ORDER BY cidade, posicao_diaria
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gráfico: top 10 bairros por receita
# MAGIC Cada barra é a receita anual mediana do bairro; ao lado, a diária e as noites ocupadas que a compõem.

# COMMAND ----------

top10 = spark.sql("""
SELECT cidade, bairro, posicao_receita, mediana_receita_anual, mediana_diaria, mediana_noites_ocupadas
FROM p1_ranking WHERE posicao_receita <= 10 ORDER BY cidade, posicao_receita
""").toPandas()

fig, eixos = novo_grafico(1, 2, largura=14, altura=5.5)
for ax, cidade in zip(eixos, ["Rio de Janeiro", "São Paulo"]):
    d = top10[top10.cidade == cidade].sort_values("posicao_receita", ascending=False)
    receita_mil = d["mediana_receita_anual"].astype(float) / 1000
    ax.barh(d["bairro"], receita_mil, color=CORES_CIDADE[cidade], height=0.62)
    for y, (v, diaria, noites) in enumerate(zip(receita_mil, d["mediana_diaria"].astype(float), d["mediana_noites_ocupadas"])):
        ax.text(v + receita_mil.max() * 0.02, y,
                f"R$ {formato_br(v, 1)} mil  ·  diária R$ {formato_br(diaria)}  ·  {noites} noites",
                va="center", fontsize=8.5, color=TINTA_SECUNDARIA)
    ax.set_xlim(0, receita_mil.max() * 1.95)
    ax.set_title(cidade, loc="left")
    ax.set_xlabel("Receita anual estimada, mediana (R$ mil)")
    ax.grid(axis="y", visible=False)
fig.suptitle("P1 · Top 10 bairros por receita anual estimada (anúncios elegíveis)", x=0.01, ha="left", fontweight="bold")
mostrar(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão P1
# MAGIC **No Rio, rentabilidade e preço andam mais juntos; em São Paulo, quem rende mais são os bairros de alta ocupação, não os mais caros.**
# MAGIC
# MAGIC - **Rio de Janeiro:** Leblon (mediana de R$ 52,1 mil/ano) e Ipanema (R$ 51,9 mil) lideram com folga, seguidos por Barra da Tijuca
# MAGIC   (R$ 33,6 mil) e Copacabana (R$ 30,6 mil). Leblon, Ipanema e Copacabana juntam diária alta com ocupação de 72 a 78 noites
# MAGIC   (a mediana dos elegíveis do RJ é 54). A
# MAGIC   correlação entre os rankings de diária e de receita é 0,62: o preço é um bom indicador, mas não suficiente. Seis bairros do top 10
# MAGIC   de diária ficam fora do top 10 de receita. O caso extremo é São Conrado: 2ª maior diária (R$ 744), mas mediana de apenas 12
# MAGIC   noites ocupadas, o que o leva à 37ª posição em receita. Joá, Lagoa, Gávea e Jardim Botânico seguem o mesmo padrão: caros e
# MAGIC   pouco ocupados (24 a 30 noites).
# MAGIC - **São Paulo:** o ranking muda de lógica. Sé (R$ 36,3 mil), Água Rasa, Santana, Barra Funda e Brás lideram com diárias medianas
# MAGIC   modestas (R$ 261 a R$ 335), mas as maiores ocupações da cidade (90 a 132 noites). A correlação entre os rankings cai para 0,37 e
# MAGIC   7 dos 10 bairros mais caros ficam fora do top 10 de receita. Socorro e Cidade Dutra, por exemplo, estão entre as diárias mais
# MAGIC   altas, mas com apenas 18 noites ocupadas.
# MAGIC - **Nas duas cidades, a ocupação mediana de um bairro praticamente não tem relação com a sua diária** (correlação de 0,10 no RJ e
# MAGIC   de −0,11 em SP): cobrar mais não reduz a ocupação de forma sistemática, e cobrar menos também não a garante.
# MAGIC
# MAGIC **Resposta:** os bairros mais rentáveis são Leblon, Ipanema, Barra da Tijuca e Copacabana no RJ, e Sé, Água Rasa, Santana, Barra
# MAGIC Funda e Brás em SP. Olhar só o preço da diária levaria a escolhas erradas, principalmente em São Paulo.
# MAGIC
# MAGIC *Limitações:* a ocupação é uma estimativa da fonte, baseada em avaliações e com teto de 255 noites; a receita é bruta (sem custos,
# MAGIC taxas ou valor do imóvel).
# MAGIC
# MAGIC *Nota:* a fonte publica os distritos de São Paulo sem acento (`Se`, `Agua Rasa`, `Bras`), grafia mantida nas tabelas e no gráfico;
# MAGIC no texto usamos a grafia oficial (Sé, Água Rasa, Brás).
