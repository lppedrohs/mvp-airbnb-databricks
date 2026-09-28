# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Carga da camada Bronze (Load)
# MAGIC
# MAGIC Lê cada arquivo do Volume e grava uma tabela Delta por **entidade × cidade** (`bronze.listings_sp`, `bronze.listings_rj`, ...).
# MAGIC
# MAGIC Decisões desta camada:
# MAGIC - **Tudo como STRING** (`inferSchema=false`): a Bronze é uma cópia fiel da fonte. A inferência automática poderia converter
# MAGIC   valores de forma silenciosa (ex.: IDs muito longos virando `double`); a tipagem fica para a Silver, depois do diagnóstico de qualidade.
# MAGIC - **Uma tabela por cidade**: preserva a rastreabilidade de cada snapshot. A centralização SP + RJ acontece na Silver.
# MAGIC - **Metadados de ingestão**: `_cidade`, `_data_snapshot`, `_arquivo_origem` e `_ingerido_em` registram a linhagem de cada linha.
# MAGIC - **Leitura de CSV robusta**: `multiLine` e `escape='"'` porque descrições e comentários têm quebras de linha e aspas.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

def ler_csv_bruto(sigla, entidade):
    return (spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", ENTIDADES[entidade]["multiline"])
            .option("quote", '"')
            .option("escape", '"')
            .load(caminho_raw(sigla, entidade))
            .select("*",
                    F.lit(sigla).alias("_cidade"),
                    F.lit(CIDADES[sigla]["snapshot"]).cast("date").alias("_data_snapshot"),
                    F.col("_metadata.file_path").alias("_arquivo_origem"),
                    F.current_timestamp().alias("_ingerido_em")))


resumo = []
for sigla in CIDADES:
    for entidade in ENTIDADES:
        destino = tabela_bronze(entidade, sigla)
        df = ler_csv_bruto(sigla, entidade)
        salvar_tabela(df, destino)
        documentar_tabela(
            destino,
            f"Bronze | Inside Airbnb - {CIDADES[sigla]['nome']} - {entidade} (snapshot {CIDADES[sigla]['snapshot']}). "
            f"Cópia fiel do arquivo {ENTIDADES[entidade]['arquivo']}, todas as colunas como STRING, "
            f"acrescida de metadados de ingestão. Fonte: insideairbnb.com (CC BY 4.0).",
            {
                "_cidade": "Sigla da cidade de origem (sp, rj).",
                "_data_snapshot": "Data de coleta do snapshot na fonte.",
                "_arquivo_origem": "Caminho do arquivo no Volume que originou a linha (linhagem).",
                "_ingerido_em": "Data e hora da carga na Bronze.",
            },
        )
        resumo.append((destino, spark.table(destino).count(), len(df.columns) - 4))

display(spark.createDataFrame(resumo, "tabela string, linhas long, colunas_da_fonte int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primeira olhada nos dados brutos
# MAGIC Amostra de colunas-chave de `listings` para entender o formato em que a fonte entrega os dados.

# COMMAND ----------

display(spark.table(tabela_bronze("listings", "sp"))
        .select("id", "host_id", "neighbourhood", "neighbourhood_cleansed", "room_type", "price",
                "minimum_nights", "host_is_superhost", "host_response_rate", "review_scores_rating", "last_scraped")
        .orderBy("id")
        .limit(10))

# COMMAND ----------

display(spark.table(tabela_bronze("calendar", "rj")).orderBy("listing_id", "date").limit(10))

# COMMAND ----------

display(spark.table(tabela_bronze("neighbourhoods", "rj")).orderBy("neighbourhood").limit(10))

# COMMAND ----------

print("Colunas de calendar:", spark.table(tabela_bronze("calendar", "rj")).columns)
print("Colunas de reviews: ", spark.table(tabela_bronze("reviews", "rj")).columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **Primeiras impressões (a medir no notebook de qualidade):**
# MAGIC - **Volume:** 42.354 anúncios em SP e 48.713 no RJ, cada um com 90 colunas. O calendário tem ~365 linhas por anúncio
# MAGIC   (SP: 15,5 mi linhas ÷ 42,4 mil anúncios), coerente com um ano de agenda por imóvel. São ~1,6 mi avaliações em SP e ~1,4 mi no RJ.
# MAGIC - `price` chega como texto, com símbolo e separador de milhar (`$358.50`): não é numérico.
# MAGIC - Campos de sim/não usam `t`/`f`. A taxa de resposta do anfitrião (`host_response_rate`) aparece vazia em toda a amostra.
# MAGIC - `id` tem números muito longos (19 dígitos): risco de perda de precisão se tratado como número decimal.
# MAGIC - `neighbourhood` está vazio na amostra; o bairro aparece em `neighbourhood_cleansed`. A lista oficial tem 96 bairros em SP e
# MAGIC   160 no RJ, com `neighbourhood_group` vazio no RJ.
# MAGIC - `last_scraped` traz datas **depois** da data do snapshot (14/06 em SP): a coleta da fonte leva alguns dias.
# MAGIC - O calendário tem **apenas 5 colunas e nenhuma de preço**: a fonte informa só a disponibilidade diária. O valor da diária
# MAGIC   terá de vir de `listings.price`.
# MAGIC - `reviews` traz `reviewer_name` e `comments`: dado pessoal e texto livre, não necessários para as perguntas.
