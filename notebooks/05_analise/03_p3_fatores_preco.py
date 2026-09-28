# Databricks notebook source
# MAGIC %md
# MAGIC # P3 · Os mesmos fatores explicam o preço no Rio e em São Paulo?
# MAGIC
# MAGIC > As características que mais influenciam o preço (localização, tipo de imóvel, capacidade, avaliação) têm o mesmo peso no Rio
# MAGIC > e em São Paulo, ou cada mercado responde a fatores diferentes?
# MAGIC
# MAGIC **Método:** para cada cidade e cada fator, medimos **quanto da variação do preço o fator explica sozinho** (η², "eta ao
# MAGIC quadrado": a parte da variância do preço que é explicada pelas diferenças entre as médias dos grupos do fator). O cálculo usa o
# MAGIC **log da diária**, porque o preço é muito assimétrico. η² vai de 0% (o fator não diferencia preços) a 100% (o fator determina o
# MAGIC preço). É uma medida descritiva, calculada fator a fator; os fatores se sobrepõem (imóveis maiores costumam ser inteiros), então
# MAGIC os percentuais não se somam.
# MAGIC
# MAGIC **Base:** anúncios elegíveis. Bairros com menos de 30 anúncios elegíveis são agrupados em "Outros", para que bairros minúsculos
# MAGIC não inflem o poder explicativo da localização.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

base = spark.sql(f"""
SELECT b.cidade, b.bairro, i.tipo_quarto, i.faixa_capacidade, i.qtd_comodidades,
       a.perfil_anfitriao, f.nota_geral, f.preco_diaria, LN(f.preco_diaria) AS log_preco
FROM {SCHEMA_GOLD}.fato_anuncio f
JOIN {SCHEMA_GOLD}.dim_bairro    b ON f.sk_bairro   = b.sk_bairro
JOIN {SCHEMA_GOLD}.dim_imovel    i ON f.id_anuncio  = i.id_anuncio
JOIN {SCHEMA_GOLD}.dim_anfitriao a ON f.id_anfitriao = a.id_anfitriao
WHERE f.elegivel_analise
""")

bairros_grandes = base.groupBy("cidade", "bairro").count().filter("count >= 30").select("cidade", "bairro", F.lit(True).alias("grande"))
por_cidade = Window.partitionBy("cidade").orderBy("qtd_comodidades")

base = (base.join(bairros_grandes, ["cidade", "bairro"], "left")
        .withColumn("localizacao_bairro", F.when(F.col("grande"), F.col("bairro")).otherwise(F.lit("Outros")))
        .withColumn("faixa_nota", F.when(F.col("nota_geral").isNull(), "sem nota")
                                   .when(F.col("nota_geral") < 4.7, "1) abaixo de 4,70")
                                   .when(F.col("nota_geral") < 4.85, "2) 4,70 a 4,84")
                                   .when(F.col("nota_geral") < 4.95, "3) 4,85 a 4,94")
                                   .otherwise("4) 4,95 a 5,00"))
        .withColumn("quartil_comodidades", F.concat(F.lit("Q"), F.ntile(4).over(por_cidade).cast("string"))))
base.createOrReplaceTempView("p3_base")

display(base.groupBy("cidade").agg(F.count("*").alias("anuncios_elegiveis"),
                                   F.countDistinct("localizacao_bairro").alias("grupos_de_bairro")).orderBy("cidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Poder explicativo de cada fator (η² sobre o log da diária)

# COMMAND ----------

FATORES = {
    "Localização (bairro)": "localizacao_bairro",
    "Tipo de acomodação": "tipo_quarto",
    "Capacidade de hóspedes": "faixa_capacidade",
    "Comodidades (quartil)": "quartil_comodidades",
    "Nota de avaliação": "faixa_nota",
    "Perfil do anfitrião": "perfil_anfitriao",
}

totais = base.groupBy("cidade").agg(F.avg("log_preco").alias("media"), F.var_pop("log_preco").alias("variancia"),
                                    F.count("*").alias("n_total"))
resultados = None
for nome, coluna in FATORES.items():
    grupos = base.groupBy("cidade", coluna).agg(F.count("*").alias("n"), F.avg("log_preco").alias("media_grupo"))
    eta2 = (grupos.join(totais, "cidade")
            .groupBy("cidade")
            .agg((F.sum(F.col("n") * (F.col("media_grupo") - F.col("media")) ** 2)
                  / (F.first("n_total") * F.first("variancia"))).alias("eta2"))
            .select("cidade", F.lit(nome).alias("fator"), F.round(100 * F.col("eta2"), 1).alias("pct_variacao_explicada")))
    resultados = eta2 if resultados is None else resultados.unionByName(eta2)

resultados.createOrReplaceTempView("p3_eta2")
display(spark.sql("""
SELECT fator,
       MAX(CASE WHEN cidade = 'Rio de Janeiro' THEN pct_variacao_explicada END) AS rio_de_janeiro_pct,
       MAX(CASE WHEN cidade = 'São Paulo'      THEN pct_variacao_explicada END) AS sao_paulo_pct
FROM p3_eta2
GROUP BY fator
ORDER BY rio_de_janeiro_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como cada fator se traduz em preço
# MAGIC Mediana da diária por grupo, para ler o tamanho dos efeitos em reais.

# COMMAND ----------

def mediana_por(coluna):
    return spark.sql(f"""
        SELECT cidade, {coluna}, COUNT(*) AS anuncios, percentile_approx(preco_diaria, 0.5) AS mediana_diaria
        FROM p3_base GROUP BY cidade, {coluna} ORDER BY cidade, {coluna}""")


display(mediana_por("tipo_quarto"))

# COMMAND ----------

display(mediana_por("faixa_capacidade"))

# COMMAND ----------

display(mediana_por("faixa_nota"))

# COMMAND ----------

# Amplitude da localização: diária mediana do bairro mais caro e do mais barato (bairros com 30+ elegíveis)
display(spark.sql("""
WITH bairros AS (
  SELECT cidade, localizacao_bairro, percentile_approx(preco_diaria, 0.5) AS mediana_diaria
  FROM p3_base WHERE localizacao_bairro <> 'Outros'
  GROUP BY cidade, localizacao_bairro)
SELECT cidade,
       COUNT(*)                                                         AS bairros,
       MIN(mediana_diaria)                                              AS bairro_mais_barato,
       MAX(mediana_diaria)                                              AS bairro_mais_caro,
       ROUND(MAX(mediana_diaria) / MIN(mediana_diaria), 1)              AS razao_mais_caro_mais_barato,
       ROUND(percentile_approx(mediana_diaria, 0.9) / percentile_approx(mediana_diaria, 0.1), 1) AS razao_p90_p10
FROM bairros GROUP BY cidade ORDER BY cidade
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gráfico: o que explica o preço em cada cidade

# COMMAND ----------

import numpy as np

eta = spark.table("p3_eta2").toPandas()
ordem = eta[eta.cidade == "Rio de Janeiro"].sort_values("pct_variacao_explicada")["fator"].tolist()
y, altura_barra = np.arange(len(ordem)), 0.38

fig, ax = novo_grafico(1, 1, largura=10, altura=5)
for i, cidade in enumerate(["Rio de Janeiro", "São Paulo"]):
    valores = eta[eta.cidade == cidade].set_index("fator").loc[ordem, "pct_variacao_explicada"].astype(float)
    barras = ax.barh(y + (0.5 - i) * altura_barra, valores, height=altura_barra * 0.92, color=CORES_CIDADE[cidade], label=cidade)
    for barra, v in zip(barras, valores):
        ax.text(v + 0.3, barra.get_y() + barra.get_height() / 2, f"{formato_br(v, 1)}%",
                va="center", fontsize=8.5, color=TINTA_SECUNDARIA)
ax.set_yticks(y, ordem)
ax.set_xlim(0, eta["pct_variacao_explicada"].astype(float).max() * 1.2)
ax.set_xlabel("% da variação do preço explicada pelo fator (η², sobre o log da diária)")
ax.grid(axis="y", visible=False)
ax.legend(loc="lower right")
ax.set_title("P3 · No RJ pesam bairro e capacidade; em SP, o tipo de acomodação", loc="left", fontsize=12)
mostrar(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão P3
# MAGIC **Não: os dois mercados precificam de forma diferente.**
# MAGIC
# MAGIC | Fator | Rio de Janeiro | São Paulo |
# MAGIC |---|---|---|
# MAGIC | Localização (bairro) | **23,7%** | 14,0% |
# MAGIC | Capacidade de hóspedes | 18,0% | 6,6% |
# MAGIC | Tipo de acomodação | 14,8% | **23,3%** |
# MAGIC | Comodidades | 3,6% | 4,8% |
# MAGIC | Perfil do anfitrião | 1,1% | 0,7% |
# MAGIC | Nota de avaliação | 0,9% | 0,3% |
# MAGIC
# MAGIC - **Rio de Janeiro: o preço é definido por onde e para quantos.** O bairro é o fator mais importante, e a diferença entre bairros é
# MAGIC   grande: a diária mediana do bairro mais caro é 7,4 vezes a do mais barato (2,6 vezes entre os percentis 90 e 10). A capacidade
# MAGIC   vem logo depois: um imóvel para 7 ou mais hóspedes custa 2,7 vezes um para 1 ou 2 (R$ 839 × R$ 310). É um mercado turístico, de
# MAGIC   praia e de grupos e famílias.
# MAGIC - **São Paulo: o preço é definido pelo tipo de hospedagem.** O que mais separa preços é ser imóvel inteiro ou quarto (R$ 338 × R$ 172).
# MAGIC   A localização pesa menos e os bairros são mais homogêneos (o mais caro custa 2,5 vezes o mais barato), e a capacidade quase não
# MAGIC   muda o preço (7 ou mais hóspedes custa só 1,5 vez o preço de 1 ou 2). É um mercado de estadias curtas e individuais, em que o
# MAGIC   imóvel típico é um estúdio ou apartamento pequeno.
# MAGIC - **Reputação e perfil do anfitrião quase não explicam o preço em nenhuma das cidades** (1% ou menos). As notas são altas e
# MAGIC   parecidas entre anúncios (a maioria acima de 4,7), e a diária de um anúncio com nota máxima é só 4% (SP) a 14% (RJ) maior que a
# MAGIC   de um com nota abaixo de 4,7.
# MAGIC
# MAGIC **Resposta:** os fatores são os mesmos, mas o peso muda. No Rio, localização e capacidade dominam; em São Paulo, o tipo de
# MAGIC acomodação é o principal fator, com a localização em segundo plano.
# MAGIC
# MAGIC *Limitações:* η² mede cada fator isoladamente; como os fatores se sobrepõem, os valores não se somam e não isolam efeitos causais.
# MAGIC O bairro tem mais grupos (51) que os demais fatores, o que tende a inflar um pouco o seu η²; com ~30 mil anúncios por cidade,
# MAGIC esse viés é pequeno.
# MAGIC Um modelo multivariado (ex.: regressão) seria o próximo passo para separar os efeitos.
