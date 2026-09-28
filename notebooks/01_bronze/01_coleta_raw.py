# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta dos arquivos brutos (Extract)
# MAGIC
# MAGIC Baixa os 8 arquivos da fonte (4 entidades × 2 cidades) para o Volume `workspace.bronze.raw_files`, sem nenhuma alteração.
# MAGIC
# MAGIC | Entidade | Arquivo | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | listings | `listings.csv.gz` | 1 linha por anúncio: imóvel, anfitrião, preço, notas |
# MAGIC | calendar | `calendar.csv.gz` | 1 linha por anúncio × dia (próximos 365 dias): disponibilidade |
# MAGIC | reviews | `reviews.csv.gz` | 1 linha por avaliação deixada por um hóspede |
# MAGIC | neighbourhoods | `neighbourhoods.csv` | lista oficial de bairros usada pela fonte |
# MAGIC
# MAGIC **Por que guardar o arquivo original antes de criar tabelas?** O arquivo no Volume funciona como evidência imutável do que a
# MAGIC fonte entregou naquela data: a Bronze pode ser reprocessada sem baixar de novo, e a pasta `<cidade>/<snapshot>/` registra a linhagem.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import os
import shutil
import urllib.request

FORCAR_DOWNLOAD = False  # True para baixar novamente mesmo que o arquivo já exista no Volume


def baixar(url, destino):
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    requisicao = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (mvp-pipeline-dados)"})
    with urllib.request.urlopen(requisicao, timeout=600) as resposta, open(destino, "wb") as arquivo:
        shutil.copyfileobj(resposta, arquivo, length=8 * 1024 * 1024)


controle = []
for sigla in CIDADES:
    for entidade in ENTIDADES:
        url, destino = url_fonte(sigla, entidade), caminho_raw(sigla, entidade)
        if os.path.exists(destino) and not FORCAR_DOWNLOAD:
            status = "já existia"
        else:
            baixar(url, destino)
            status = "baixado"
        tamanho_mb = round(os.path.getsize(destino) / 1024 ** 2, 2)
        controle.append((sigla, entidade, url, destino, tamanho_mb, status))
        print(f"{sigla}/{entidade:<15} {tamanho_mb:>8} MB  {status}")

display(spark.createDataFrame(
    controle, "cidade string, entidade string, url_origem string, caminho_volume string, tamanho_mb double, status string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plano B: envio pelo Databricks CLI
# MAGIC Se o compute não tiver acesso de saída à internet (erro `URLError`/`403` na célula acima), os mesmos arquivos podem ser
# MAGIC baixados na máquina local e enviados ao Volume com o script [`scripts/coleta_via_cli.ps1`](../../scripts/coleta_via_cli.ps1),
# MAGIC que mantém a mesma estrutura de pastas. Depois disso, a célula acima mostra os arquivos com status `já existia`.

# COMMAND ----------

display(dbutils.fs.ls(VOLUME_RAW))
