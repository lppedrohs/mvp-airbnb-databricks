# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · Dimensões
# MAGIC
# MAGIC A Gold organiza os dados em um **esquema estrela com mais de uma tabela fato** (constelação): as tabelas fato guardam as medidas
# MAGIC (preço, ocupação, receita, disponibilidade, avaliações) e compartilham as dimensões abaixo, que descrevem o contexto.
# MAGIC
# MAGIC ```
# MAGIC                       dim_bairro
# MAGIC                           │
# MAGIC   dim_anfitriao ──── fato_anuncio ──── dim_imovel
# MAGIC                           │
# MAGIC            ┌──────────────┴──────────────┐
# MAGIC   fato_disponibilidade_mensal       fato_avaliacao
# MAGIC            └────────── dim_data ─────────┘
# MAGIC ```
# MAGIC
# MAGIC | Dimensão | Grão | Origem | Serve a |
# MAGIC |---|---|---|---|
# MAGIC | `dim_bairro` | cidade + bairro | `silver.bairros` | P1, P2, P3 |
# MAGIC | `dim_anfitriao` | anfitrião | `silver.anuncios` agregado (SP + RJ juntos) | P2 |
# MAGIC | `dim_imovel` | anúncio | `silver.anuncios` (atributos descritivos) | P3 |
# MAGIC | `dim_data` | dia | sequência de datas gerada | fatos temporais |

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

def tabela_gold(nome):
    return f"{SCHEMA_GOLD}.{nome}"


anuncios = spark.table(tabela_silver("anuncios"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_bairro
# MAGIC Chave substituta `sk_bairro` gerada por hash de cidade + bairro: é determinística (o mesmo bairro recebe sempre a mesma chave
# MAGIC a cada execução) e evita juntar as tabelas fato por dois campos de texto.

# COMMAND ----------

dim_bairro = (spark.table(tabela_silver("bairros"))
              .select(F.xxhash64("sigla_cidade", "bairro").alias("sk_bairro"),
                      "sigla_cidade", "cidade", "bairro", "grupo_bairro"))

destino = tabela_gold("dim_bairro")
salvar_tabela(dim_bairro, destino)
validar_chave(destino, ["sk_bairro"])
documentar_tabela(destino,
    "Gold | Dimensão de localização: bairros oficiais de São Paulo e Rio de Janeiro. Grão: 1 linha por cidade + bairro. "
    "Origem: silver.bairros.",
    {
        "sk_bairro": "Chave substituta do bairro (hash xxhash64 de sigla_cidade + bairro). Derivada.",
        "sigla_cidade": "Sigla da cidade. Domínio: sp, rj.",
        "cidade": "Nome da cidade. Domínio: São Paulo, Rio de Janeiro.",
        "bairro": "Nome oficial do bairro (96 em SP, 160 no RJ). Origem: silver.bairros.bairro.",
        "grupo_bairro": "Subprefeitura em SP (32 valores); NULL no RJ. Origem: silver.bairros.grupo_bairro.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_anfitriao
# MAGIC O portfólio é **recalculado sobre a base unificada SP + RJ** (DQ-09): um anfitrião com 3 anúncios em SP e 3 no RJ tem 6 anúncios,
# MAGIC e não 3 em cada cidade. O perfil segue as faixas definidas no planejamento:
# MAGIC
# MAGIC | Perfil | Anúncios na base |
# MAGIC |---|---|
# MAGIC | 1 - Ocasional | 1 |
# MAGIC | 2 - Pequeno | 2 a 4 |
# MAGIC | 3 - Profissional | 5 ou mais |
# MAGIC
# MAGIC Atributos do anfitrião repetidos em cada anúncio são consolidados: o selo Superhost é igual em todos os anúncios de um mesmo
# MAGIC anfitrião, e o tempo como anfitrião difere em 64 casos (coletas em dias diferentes), resolvidos pelo maior valor.

# COMMAND ----------

dim_anfitriao = (anuncios.groupBy("id_anfitriao").agg(
        F.countDistinct("id_anuncio").alias("qtd_anuncios_total"),
        F.sum((F.col("sigla_cidade") == "sp").cast("int")).alias("qtd_anuncios_sp"),
        F.sum((F.col("sigla_cidade") == "rj").cast("int")).alias("qtd_anuncios_rj"),
        F.countDistinct("sigla_cidade").alias("qtd_cidades"),
        F.max("anfitriao_superhost").alias("anfitriao_superhost"),
        F.max("anfitriao_identidade_verificada").alias("anfitriao_identidade_verificada"),
        F.max("meses_como_anfitriao").alias("meses_como_anfitriao"))
    .withColumn("perfil_anfitriao",
                F.when(F.col("qtd_anuncios_total") == 1, "1 - Ocasional")
                 .when(F.col("qtd_anuncios_total") <= 4, "2 - Pequeno")
                 .otherwise("3 - Profissional")))

destino = tabela_gold("dim_anfitriao")
salvar_tabela(dim_anfitriao, destino)
validar_chave(destino, ["id_anfitriao"])
documentar_tabela(destino,
    "Gold | Dimensão de anfitrião, com portfólio recalculado sobre a base unificada SP + RJ (DQ-09). Grão: 1 linha por anfitrião. "
    "Origem: silver.anuncios agregado por id_anfitriao.",
    {
        "id_anfitriao": "Identificador do anfitrião (chave). Origem: silver.anuncios.id_anfitriao.",
        "qtd_anuncios_total": "Anúncios do anfitrião nas duas cidades (>= 1). Derivada: contagem em silver.anuncios.",
        "qtd_anuncios_sp": "Anúncios do anfitrião em São Paulo (>= 0). Derivada.",
        "qtd_anuncios_rj": "Anúncios do anfitrião no Rio de Janeiro (>= 0). Derivada.",
        "qtd_cidades": "Em quantas cidades o anfitrião atua. Domínio: 1 ou 2. Derivada.",
        "anfitriao_superhost": "Anfitrião com selo Superhost (boolean). Origem: silver.anuncios.anfitriao_superhost.",
        "anfitriao_identidade_verificada": "Identidade verificada pela plataforma (boolean). Origem: silver.anuncios.",
        "meses_como_anfitriao": "Tempo como anfitrião em meses (maior valor entre os anúncios). Origem: silver.anuncios.",
        "perfil_anfitriao": "Perfil pelo tamanho do portfólio. Domínio: 1 - Ocasional (1 anúncio), 2 - Pequeno (2 a 4), 3 - Profissional (5 ou mais). Derivada.",
    })

display(spark.table(destino).groupBy("perfil_anfitriao").agg(
    F.count("*").alias("anfitrioes"), F.sum("qtd_anuncios_total").alias("anuncios")).orderBy("perfil_anfitriao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_imovel
# MAGIC Atributos físicos e de tipo do anúncio, mais a faixa de capacidade, usada para comparar preços entre imóveis de porte parecido (P3).

# COMMAND ----------

dim_imovel = anuncios.select(
    "id_anuncio", "tipo_propriedade", "tipo_quarto", "capacidade_hospedes",
    F.when(F.col("capacidade_hospedes") <= 2, "1) 1 a 2")
     .when(F.col("capacidade_hospedes") <= 4, "2) 3 a 4")
     .when(F.col("capacidade_hospedes") <= 6, "3) 5 a 6")
     .otherwise("4) 7 ou mais").alias("faixa_capacidade"),
    "quartos", "camas", "banheiros", "qtd_comodidades")

destino = tabela_gold("dim_imovel")
salvar_tabela(dim_imovel, destino)
validar_chave(destino, ["id_anuncio"])
documentar_tabela(destino,
    "Gold | Dimensão do imóvel anunciado: tipo e características físicas. Grão: 1 linha por anúncio. Origem: silver.anuncios.",
    {
        "id_anuncio": "Identificador do anúncio (chave). Origem: silver.anuncios.id_anuncio.",
        "tipo_propriedade": "Tipo de propriedade declarado (ex.: Entire rental unit). Origem: silver.anuncios.",
        "tipo_quarto": "Tipo de acomodação. Domínio: Entire home/apt, Private room, Shared room, Hotel room. Origem: silver.anuncios.",
        "capacidade_hospedes": "Número máximo de hóspedes (1 a 16). Origem: silver.anuncios.",
        "faixa_capacidade": "Faixa de capacidade. Domínio: 1) 1 a 2, 2) 3 a 4, 3) 5 a 6, 4) 7 ou mais. Derivada.",
        "quartos": "Número de quartos (pode ser NULL). Origem: silver.anuncios.",
        "camas": "Número de camas (pode ser NULL). Origem: silver.anuncios.",
        "banheiros": "Número de banheiros (pode ser NULL). Origem: silver.anuncios.",
        "qtd_comodidades": "Quantidade de comodidades listadas. Origem: silver.anuncios.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_data
# MAGIC Calendário gerado do dia da avaliação mais antiga até o último dia do calendário de disponibilidade, cobrindo as datas das
# MAGIC duas tabelas fato temporais.

# COMMAND ----------

limites = (spark.table(tabela_silver("avaliacoes")).agg(F.min("data_avaliacao").alias("d")).first()["d"],
           spark.table(tabela_silver("calendario")).agg(F.max("data").alias("d")).first()["d"])
print("Período da dim_data:", limites)

dias_semana = F.array(*[F.lit(d) for d in ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]])
dim_data = (spark.range(1)
            .select(F.explode(F.sequence(F.lit(limites[0]), F.lit(limites[1]))).alias("data"))
            .select("data",
                    F.year("data").alias("ano"),
                    F.month("data").alias("mes"),
                    F.date_format("data", "yyyy-MM").alias("ano_mes"),
                    F.quarter("data").alias("trimestre"),
                    F.dayofweek("data").alias("dia_semana"),
                    F.element_at(dias_semana, F.dayofweek("data")).alias("nome_dia_semana"),
                    F.dayofweek("data").isin(1, 7).alias("fim_de_semana")))

destino = tabela_gold("dim_data")
salvar_tabela(dim_data, destino)
validar_chave(destino, ["data"])
documentar_tabela(destino,
    "Gold | Dimensão de calendário. Grão: 1 linha por dia, da avaliação mais antiga ao fim do calendário de disponibilidade. Gerada.",
    {
        "data": "Dia (chave).",
        "ano": "Ano.",
        "mes": "Mês (1 a 12).",
        "ano_mes": "Ano e mês no formato yyyy-MM.",
        "trimestre": "Trimestre (1 a 4).",
        "dia_semana": "Dia da semana (1 = domingo ... 7 = sábado).",
        "nome_dia_semana": "Nome do dia da semana em português.",
        "fim_de_semana": "true para sábado e domingo.",
    })
