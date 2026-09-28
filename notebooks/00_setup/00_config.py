# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração do projeto
# MAGIC
# MAGIC Parâmetros e funções utilitárias compartilhadas por todo o pipeline. Os demais notebooks carregam este arquivo com
# MAGIC `%run ../00_setup/00_config`, assim caminhos, nomes de tabelas e regras de conversão ficam definidos **em um único lugar**.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Fonte | [Inside Airbnb](https://insideairbnb.com/get-the-data/) — licença **CC BY 4.0** (uso livre com atribuição) |
# MAGIC | Cidades | São Paulo (snapshot 2026-06-14) e Rio de Janeiro (snapshot 2026-06-24) |
# MAGIC | Catálogo | `workspace` (padrão do Databricks Free Edition) |
# MAGIC | Camadas | schemas `bronze`, `silver`, `gold` (arquitetura medalhão) |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------- Catálogo e camadas ----------------
CATALOGO = "workspace"
SCHEMA_BRONZE = f"{CATALOGO}.bronze"
SCHEMA_SILVER = f"{CATALOGO}.silver"
SCHEMA_GOLD = f"{CATALOGO}.gold"

# Volume do Unity Catalog onde os arquivos originais da fonte são armazenados (zona de pouso)
VOLUME_RAW = f"/Volumes/{CATALOGO}/bronze/raw_files"

# ---------------- Fonte: Inside Airbnb ----------------
# Cada cidade tem seu próprio snapshot (data de coleta). O bbox (lat_min, lat_max, lon_min, lon_max)
# delimita o município e é usado como checagem de acurácia das coordenadas.
CIDADES = {
    "sp": {
        "nome": "São Paulo",
        "snapshot": "2026-06-14",
        "url_base": "https://data.insideairbnb.com/brazil/sp/s%C3%A3o-paulo/2026-06-14",
        "bbox": (-24.01, -23.35, -46.83, -46.36),
    },
    "rj": {
        "nome": "Rio de Janeiro",
        "snapshot": "2026-06-24",
        "url_base": "https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2026-06-24",
        "bbox": (-23.09, -22.74, -43.80, -43.09),
    },
}

# Arquivos coletados por cidade. multiline=True nos arquivos com texto livre (descrições e comentários com quebra de linha).
ENTIDADES = {
    "listings":       {"arquivo": "data/listings.csv.gz",              "multiline": True},
    "calendar":       {"arquivo": "data/calendar.csv.gz",              "multiline": False},
    "reviews":        {"arquivo": "data/reviews.csv.gz",               "multiline": True},
    "neighbourhoods": {"arquivo": "visualisations/neighbourhoods.csv", "multiline": False},
}

# COMMAND ----------

# ---------------- Caminhos e nomes ----------------
def url_fonte(sigla, entidade):
    return f"{CIDADES[sigla]['url_base']}/{ENTIDADES[entidade]['arquivo']}"


def caminho_raw(sigla, entidade):
    """Arquivo no Volume, organizado por cidade/data do snapshot: raw_files/sp/2026-06-14/listings.csv.gz"""
    nome_arquivo = ENTIDADES[entidade]["arquivo"].split("/")[-1]
    return f"{VOLUME_RAW}/{sigla}/{CIDADES[sigla]['snapshot']}/{nome_arquivo}"


def tabela_bronze(entidade, sigla):
    return f"{SCHEMA_BRONZE}.{entidade}_{sigla}"


def tabela_silver(nome):
    return f"{SCHEMA_SILVER}.{nome}"


def ler_bronze_unificada(entidade):
    """União SP + RJ de uma entidade da Bronze. unionByName tolera colunas diferentes entre snapshots."""
    dfs = [spark.table(tabela_bronze(entidade, sigla)) for sigla in CIDADES]
    df = dfs[0]
    for outro in dfs[1:]:
        df = df.unionByName(outro, allowMissingColumns=True)
    return df


def col_nome_cidade(coluna="_cidade"):
    """Converte a sigla (sp/rj) no nome da cidade."""
    mapa = F.create_map(*[F.lit(x) for sigla, c in CIDADES.items() for x in (sigla, c["nome"])])
    return mapa[F.col(coluna)]

# COMMAND ----------

# ---------------- Conversão de tipos (usadas na Silver) ----------------
# try_cast devolve NULL em vez de falhar quando o valor não é convertível (o serverless roda em modo ANSI).
def para_bigint(c):
    return F.expr(f"try_cast(`{c}` AS BIGINT)")


def para_double(c):
    return F.expr(f"try_cast(`{c}` AS DOUBLE)")


def para_inteiro(c):
    return F.expr(f"try_cast(try_cast(`{c}` AS DOUBLE) AS INT)")


def para_data(c):
    return F.expr(f"try_cast(`{c}` AS DATE)")


def parse_preco(c):
    """'$1,234.00' -> 1234.00. Apesar do símbolo '$', a fonte publica o valor na moeda local (BRL)."""
    return F.expr(f"try_cast(regexp_replace(`{c}`, '[$,]', '') AS DECIMAL(12,2))")


def parse_percentual(c):
    """'95%' -> 0.95 ; 'N/A' -> NULL"""
    return F.expr(f"try_cast(regexp_replace(`{c}`, '%', '') AS DOUBLE) / 100")


def parse_booleano(c):
    """'t'/'f' -> true/false ; qualquer outro valor -> NULL"""
    return F.when(F.col(c) == "t", True).when(F.col(c) == "f", False)

# COMMAND ----------

# ---------------- Qualidade, persistência e catálogo ----------------
def perfil_completude(df, colunas=None):
    """% de valores nulos ou vazios por coluna."""
    colunas = [c for c in (colunas or df.columns) if c in df.columns]
    total = df.count()
    faltantes = df.agg(*[
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1).otherwise(0)).alias(c)
        for c in colunas
    ]).first().asDict()
    linhas = [(c, total, int(faltantes[c] or 0), round(100 * (faltantes[c] or 0) / total, 2) if total else 0.0)
              for c in colunas]
    return (spark.createDataFrame(linhas, "coluna string, total_linhas long, nulos_ou_vazios long, pct_nulos double")
            .orderBy(F.desc("pct_nulos"), "coluna"))


def contar_duplicados(df, chaves):
    """Quantidade de chaves que aparecem mais de uma vez."""
    return df.groupBy(*chaves).count().filter("count > 1").count()


def registrar_impacto(etapa, antes, depois):
    print(f"{etapa:<60} {antes:>12,} -> {depois:>12,}   removidos: {antes - depois:,}")


def salvar_tabela(df, nome_tabela):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(nome_tabela))


def validar_chave(nome_tabela, chaves):
    """Validação pós-carga: a chave da tabela deve ser única e não nula."""
    df = spark.table(nome_tabela)
    nulos = df.filter(" OR ".join(f"{c} IS NULL" for c in chaves)).count()
    duplicados = contar_duplicados(df, chaves)
    assert nulos == 0 and duplicados == 0, f"{nome_tabela}: {nulos} chaves nulas e {duplicados} duplicadas"
    print(f"OK {nome_tabela}: chave {chaves} única e não nula ({df.count():,} linhas)")


def _escapar(texto):
    return texto.replace("\\", "\\\\").replace("'", "\\'")


def documentar_tabela(nome_tabela, descricao, colunas):
    """Grava descrição da tabela e das colunas no Unity Catalog (catálogo de dados vivo, junto do código)."""
    spark.sql(f"COMMENT ON TABLE {nome_tabela} IS '{_escapar(descricao)}'")
    for coluna, desc in colunas.items():
        spark.sql(f"ALTER TABLE {nome_tabela} ALTER COLUMN {coluna} COMMENT '{_escapar(desc)}'")
    print(f"Catálogo atualizado: {nome_tabela} ({len(colunas)} colunas documentadas)")


# ---------------- Gráficos (usados nas análises) ----------------
# Paleta categórica validada para daltonismo: a cor identifica a cidade em todos os gráficos.
CORES_CIDADE = {"Rio de Janeiro": "#2a78d6", "São Paulo": "#eb6834"}
TINTA_SECUNDARIA = "#52514e"


def formato_br(valor, casas=0):
    """1234.5 -> '1.234,5'"""
    return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def novo_grafico(linhas=1, colunas=1, largura=12, altura=5):
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
        "axes.edgecolor": "#c3c2b7", "axes.labelcolor": TINTA_SECUNDARIA, "axes.titlecolor": "#0b0b0b",
        "xtick.color": "#898781", "ytick.color": TINTA_SECUNDARIA, "text.color": "#0b0b0b",
        "axes.grid": True, "grid.color": "#e1e0d9", "grid.linewidth": 0.8, "axes.axisbelow": True,
        "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
        "axes.titlesize": 11, "axes.titleweight": "bold", "legend.frameon": False,
    })
    if "text.parse_math" in plt.rcParams:
        plt.rcParams["text.parse_math"] = False   # "R$" não deve virar fórmula matemática
    return plt.subplots(linhas, colunas, figsize=(largura, altura))


def mostrar(fig):
    import matplotlib.pyplot as plt
    fig.tight_layout()
    plt.show()


print(f"Configuração carregada | catálogo={CATALOGO} | cidades={list(CIDADES)} | entidades={list(ENTIDADES)}")
