# Databricks notebook source
# MAGIC %md
# MAGIC # P2 · Anfitrião profissional × anfitrião ocasional
# MAGIC
# MAGIC > Anfitriões com vários imóveis praticam preços e recebem avaliações diferentes dos anfitriões com um único imóvel?
# MAGIC > Esse perfil está concentrado em bairros específicos?
# MAGIC
# MAGIC **Perfis** (`gold.dim_anfitriao`, portfólio contado com SP e RJ juntos): 1 - Ocasional (1 anúncio), 2 - Pequeno (2 a 4),
# MAGIC 3 - Profissional (5 ou mais). A participação na oferta usa todos os anúncios; preço, ocupação e nota usam só os elegíveis.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW p2_base AS
SELECT f.*, a.perfil_anfitriao, a.anfitriao_superhost, b.cidade, b.bairro, i.tipo_quarto
FROM {SCHEMA_GOLD}.fato_anuncio f
JOIN {SCHEMA_GOLD}.dim_anfitriao a ON f.id_anfitriao = a.id_anfitriao
JOIN {SCHEMA_GOLD}.dim_bairro    b ON f.sk_bairro   = b.sk_bairro
JOIN {SCHEMA_GOLD}.dim_imovel    i ON f.id_anuncio  = i.id_anuncio
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quem controla a oferta?

# COMMAND ----------

display(spark.sql("""
SELECT cidade, perfil_anfitriao,
       COUNT(DISTINCT id_anfitriao)                                                   AS anfitrioes,
       COUNT(*)                                                                       AS anuncios,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cidade), 1)            AS pct_anuncios_da_cidade,
       ROUND(100 * COUNT(DISTINCT id_anfitriao)
             / SUM(COUNT(DISTINCT id_anfitriao)) OVER (PARTITION BY cidade), 1)       AS pct_anfitrioes_da_cidade
FROM p2_base
GROUP BY cidade, perfil_anfitriao
ORDER BY cidade, perfil_anfitriao
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preço, ocupação, receita e reputação por perfil (anúncios elegíveis)

# COMMAND ----------

display(spark.sql("""
SELECT cidade, perfil_anfitriao,
       COUNT(*)                                                         AS anuncios_elegiveis,
       percentile_approx(preco_diaria, 0.5)                             AS mediana_diaria,
       percentile_approx(noites_ocupadas_estimadas_12m, 0.5)            AS mediana_noites_ocupadas,
       percentile_approx(receita_estimada_12m, 0.5)                     AS mediana_receita_anual,
       ROUND(AVG(nota_geral), 3)                                        AS nota_media,
       ROUND(100 * AVG(CASE WHEN nota_geral >= 4.8 THEN 1 ELSE 0 END), 1) AS pct_nota_4_8_ou_mais,
       ROUND(100 * AVG(CAST(anfitriao_superhost AS INT)), 1)            AS pct_superhost,
       ROUND(100 * AVG(CASE WHEN tipo_quarto = 'Entire home/apt' THEN 1 ELSE 0 END), 1) AS pct_imovel_inteiro
FROM p2_base
WHERE elegivel_analise
GROUP BY cidade, perfil_anfitriao
ORDER BY cidade, perfil_anfitriao
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparação justa: só imóveis inteiros
# MAGIC Como os perfis podem ter misturas diferentes de imóveis inteiros e quartos, repetimos a comparação apenas para
# MAGIC `Entire home/apt`, o tipo dominante nas duas cidades.

# COMMAND ----------

display(spark.sql("""
SELECT cidade, perfil_anfitriao,
       COUNT(*)                                              AS anuncios_elegiveis,
       percentile_approx(preco_diaria, 0.5)                  AS mediana_diaria,
       percentile_approx(noites_ocupadas_estimadas_12m, 0.5) AS mediana_noites_ocupadas,
       percentile_approx(receita_estimada_12m, 0.5)          AS mediana_receita_anual,
       ROUND(AVG(nota_geral), 3)                             AS nota_media
FROM p2_base
WHERE elegivel_analise AND tipo_quarto = 'Entire home/apt'
GROUP BY cidade, perfil_anfitriao
ORDER BY cidade, perfil_anfitriao
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Onde os profissionais se concentram?
# MAGIC Participação dos anúncios de anfitriões profissionais em cada bairro (bairros com pelo menos 30 anúncios).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW p2_bairros AS
SELECT cidade, bairro,
       COUNT(*)                                                                         AS anuncios,
       ROUND(100 * AVG(CASE WHEN perfil_anfitriao = '3 - Profissional' THEN 1 ELSE 0 END), 1) AS pct_anuncios_profissionais
FROM p2_base
GROUP BY cidade, bairro
HAVING COUNT(*) >= 30
""")

display(spark.sql("""
SELECT * FROM (
  SELECT *, RANK() OVER (PARTITION BY cidade ORDER BY pct_anuncios_profissionais DESC, bairro) AS posicao
  FROM p2_bairros)
WHERE posicao <= 10
ORDER BY cidade, posicao
"""))

# COMMAND ----------

display(spark.sql("""
SELECT cidade,
       COUNT(*)                                                    AS bairros,
       percentile_approx(pct_anuncios_profissionais, 0.5)          AS mediana_pct_profissionais,
       MIN(pct_anuncios_profissionais)                             AS minimo,
       MAX(pct_anuncios_profissionais)                             AS maximo,
       ROUND(corr(anuncios, pct_anuncios_profissionais), 2)        AS correlacao_tamanho_bairro_x_pct_profissionais
FROM p2_bairros GROUP BY cidade ORDER BY cidade
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gráfico: peso na oferta × reputação, por perfil

# COMMAND ----------

import numpy as np

oferta = spark.sql("""
SELECT cidade, perfil_anfitriao, 100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cidade) AS valor
FROM p2_base GROUP BY cidade, perfil_anfitriao""").toPandas()
reputacao = spark.sql("""
SELECT cidade, perfil_anfitriao, 100 * AVG(CASE WHEN nota_geral >= 4.8 THEN 1 ELSE 0 END) AS valor
FROM p2_base WHERE elegivel_analise GROUP BY cidade, perfil_anfitriao""").toPandas()

perfis = ["1 - Ocasional", "2 - Pequeno", "3 - Profissional"]
rotulos = ["Ocasional\n(1 anúncio)", "Pequeno\n(2 a 4)", "Profissional\n(5 ou mais)"]
x, largura = np.arange(len(perfis)), 0.38

fig, eixos = novo_grafico(1, 2, largura=13, altura=4.8)
paineis = [(oferta, "Participação na oferta (% dos anúncios da cidade)"),
           (reputacao, "Anúncios com nota ≥ 4,8 (% dos elegíveis)")]
for ax, (tabela, titulo) in zip(eixos, paineis):
    for i, cidade in enumerate(["Rio de Janeiro", "São Paulo"]):
        valores = tabela[tabela.cidade == cidade].set_index("perfil_anfitriao").loc[perfis, "valor"].astype(float)
        barras = ax.bar(x + (i - 0.5) * largura, valores, width=largura * 0.92, color=CORES_CIDADE[cidade], label=cidade)
        for barra, v in zip(barras, valores):
            ax.text(barra.get_x() + barra.get_width() / 2, v + 1.5, f"{formato_br(v, 1)}%",
                    ha="center", fontsize=8.5, color=TINTA_SECUNDARIA)
    ax.set_xticks(x, rotulos)
    ax.set_ylim(0, 100)
    ax.set_title(titulo, loc="left")
    ax.grid(axis="x", visible=False)
eixos[0].legend(loc="upper left")
fig.suptitle("P2 · Profissionais concentram a oferta, mas têm notas mais baixas", x=0.01, ha="left", fontweight="bold")
mostrar(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão P2
# MAGIC **Os profissionais são poucos, mas controlam grande parte da oferta, sobretudo em São Paulo. Eles não cobram diárias muito
# MAGIC diferentes, mas têm avaliações sistematicamente piores.**
# MAGIC
# MAGIC - **Concentração da oferta:** anfitriões com 5 ou mais anúncios são só 4,5% dos anfitriões do RJ e 7,6% dos de SP, mas detêm
# MAGIC   **30,4% dos anúncios do RJ e 54,2% dos de SP**. Em São Paulo, mais da metade da oferta está nas mãos de ~1.200 operadores.
# MAGIC - **Preço:** a diária mediana praticamente não muda com o perfil (RJ: R$ 439 ocasional × R$ 407 profissional; SP: R$ 318 × R$ 335).
# MAGIC   Comparando só imóveis inteiros, a diferença fica abaixo de 4% nas duas cidades.
# MAGIC - **Ocupação e receita:** no RJ, os profissionais ocupam mais (mediana de 66 noites contra 48 dos ocasionais) e faturam mais
# MAGIC   (R$ 27,1 mil × R$ 21,3 mil; +38% considerando só imóveis inteiros). Em SP não há vantagem: os profissionais ocupam 66 noites
# MAGIC   contra 72 dos ocasionais, com receitas equivalentes.
# MAGIC - **Avaliação:** é a diferença mais consistente. A nota média cai de 4,86 (ocasional) para 4,75 (profissional) no RJ, e de 4,88
# MAGIC   para 4,72 em SP; a parcela de anúncios com nota 4,8 ou mais cai de 80% para 61% no RJ e de 85% para 57% em SP. Já o selo
# MAGIC   Superhost é igual ou mais frequente entre pequenos e profissionais (RJ: 53% dos profissionais contra 39% dos ocasionais).
# MAGIC - **Onde estão:** em SP, a presença profissional é alta em toda a cidade (mediana de 43% dos anúncios por bairro) e maior nos
# MAGIC   bairros com mais oferta (correlação de 0,49 entre tamanho do bairro e participação profissional): Moema (67%), Itaim Bibi (65%),
# MAGIC   Vila Mariana (60%), Jardim Paulista (59%) e Pinheiros (58%). No RJ, a presença é menor (mediana de 24%) e aparece em Ipanema (43%),
# MAGIC   Leblon (41%) e Centro (38%). Os primeiros do ranking do RJ (Encantado, Joá e Marechal Hermes, com 30 a 174 anúncios) são
# MAGIC   bairros pequenos e devem ser lidos com cautela.
# MAGIC
# MAGIC **Resposta:** sim, o perfil profissional se diferencia, mas principalmente na escala e na reputação, não no preço. A concentração é
# MAGIC forte nos bairros mais disputados de São Paulo e na Zona Sul do Rio.
