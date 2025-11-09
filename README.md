
# 🧱 Projeto Medalhão - Pipeline Delta Lake (IPCA, Boi Gordo e Insights Econômicos)

## 📘 Descrição Geral

Este projeto implementa um pipeline completo no modelo **Medalhão (Bronze → Silver → Gold)** utilizando **Delta Lake** no **Databricks**.  
O objetivo é coletar, integrar e transformar dados econômicos provenientes do **Banco Central do Brasil (IPCA)** e do **Indicador Boi Gordo**, realizando cálculos de variações e consolidação de insights.

Toda a orquestração é feita dentro do **Databricks Workspace**, com versionamento, atualização incremental via **MERGE (UPSERT)** e camadas Delta bem definidas.

---

## 🏗️ Estrutura de Camadas

| Camada | Nome no Workspace | Descrição |
|--------|------------------|------------|
| 🥉 **Bronze** | `workspace.bronze_etl` | Armazena dados brutos coletados diretamente das APIs e arquivos CSV externos. |
| 🥈 **Silver** | `workspace.silver_etl` | Contém dados tratados e integrados (IPCA + Boi Gordo). |
| 🥇 **Gold** | `workspace.gold_etl` | Camada final com indicadores e variações percentuais consolidadas. |

Criação dos schemas:
```sql
CREATE SCHEMA IF NOT EXISTS workspace.bronze_etl COMMENT 'Camada Bronze para armazenamento de dados brutos';
CREATE SCHEMA IF NOT EXISTS workspace.silver_etl COMMENT 'Camada Silver para armazenamento de dados tratados';
CREATE SCHEMA IF NOT EXISTS workspace.gold_etl COMMENT 'Camada Gold para armazenamento de dados prontos para uso';
```

---

## ⚙️ Função Genérica: `merge_delta_table()`

A função `merge_delta_table` é usada por todas as camadas para realizar operações **incrementais e idempotentes** no Delta Lake.  
Agora, ela inclui uma lógica inteligente de chaves de mesclagem (MERGE) conforme o tipo de tabela.

### 📜 Implementação Atualizada

```python
from delta.tables import DeltaTable

def merge_delta_table(df_join, tabela_destino):
    """
    Realiza MERGE (upsert) de um DataFrame em uma tabela Delta Lake.

    Regras de chaves:
    -----------------
    - Para 'bronze_etl.ipca' ou 'bronze_etl.boi_gordo': usa as colunas 'data', 'valor'
    - Para demais tabelas: usa as colunas 'data', 'ipca', 'boi_gordo'

    Parâmetros:
    -----------
    df_join : DataFrame
        DataFrame com os dados a serem inseridos/atualizados.
    tabela_destino : str
        Nome completo da tabela Delta (ex: 'bronze_etl.ipca' ou 'gold_etl.insights').

    Retorno:
    --------
    None
    """

    if df_join is not None and df_join.limit(1).count() > 0:
        total = df_join.count()
        print(f"✅ Total de registros carregados: {total}")

        if spark.catalog.tableExists(tabela_destino):
            print(f"📦 Tabela {tabela_destino} já existe — atualizando dados...")

            delta_table = DeltaTable.forName(spark, tabela_destino)

            # 🔑 Define a condição de merge conforme a tabela
            if tabela_destino in ["bronze_etl.ipca", "bronze_etl.boi_gordo"]:
                condicao_merge = "t.data = s.data AND t.valor = s.valor"
                print("🔑 Usando chaves: data, valor")
            else:
                condicao_merge = (
                    "t.data = s.data "
                    "AND t.ipca = s.ipca "
                    "AND t.boi_gordo = s.boi_gordo"
                )
                print("🔑 Usando chaves: data, ipca, boi_gordo")

            # 🚀 Executa o MERGE
            (
                delta_table.alias("t")
                .merge(
                    df_join.alias("s"),
                    condicao_merge
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(f"✅ MERGE concluído com sucesso em {tabela_destino}")

        else:
            print(f"🆕 Tabela {tabela_destino} não existe — criando nova tabela...")

            df_join.write                 .format("delta")                 .partitionBy("data")                 .mode("append")                 .option("overwriteSchema", "true")                 .saveAsTable(tabela_destino)

            print(f"✅ Tabela {tabela_destino} criada e dados inseridos com sucesso.")

    else:
        print("⚠️ Nenhum dado para atualizar.")
```

---

## 🥉 Camada Bronze

### 📊 1. IPCA — Coleta de Dados do Banco Central

```python
URL = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados'
params = {'formato': 'json', 'dataInicial': '01/01/2024', 'dataFinal': datetime.now().strftime('%d/%m/%Y')}
response = requests.get(URL, params=params)
data = response.json()
df = spark.createDataFrame(data).toDF('data', 'valor')
df = df.withColumn("valor", col("valor").cast("double"))
df = df.withColumn("data_coleta", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))
merge_delta_table(df, "workspace.bronze_etl.ipca")
```

| Coluna | Tipo | Descrição |
|---------|------|-----------|
| `data` | date | Data de referência do IPCA |
| `valor` | double | Valor percentual do IPCA |
| `data_coleta` | timestamp | Data/hora da coleta (fuso São Paulo) |

---

### 🐂 2. Boi Gordo — Leitura e Gravação de Dados CSV

Fonte: `/Volumes/workspace/bronze_etl/boigordo/BoiGordo.csv`

```python
df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/bronze_etl/boigordo/BoiGordo.csv")
)

df = df.withColumnRenamed("Valor", "valor")
df = df.withColumn(
    "data_coleta",
    from_utc_timestamp(current_timestamp(), "America/Sao_Paulo")
)

merge_delta_table(df, "workspace.bronze_etl.boi_gordo")
```

| Coluna | Tipo | Descrição |
|---------|------|-----------|
| `Data` | string | Mês/Ano da cotação |
| `valor` | double | Preço médio do boi gordo |
| `data_coleta` | timestamp | Data/hora da coleta ajustada para o fuso horário de São Paulo |

---

## 🥈 Camada Silver — Integração Econômica (IPCA + Boi Gordo)

*(mantida conforme versão anterior, com join, padronização de datas e deduplicação)*

---

## 🥇 Camada Gold — Insights Econômicos (Variação Percentual)

*(mantida conforme versão anterior, com cálculo de variação percentual mensal e MERGE final)*

---

## 🔁 Fluxo de Dados

```mermaid
graph TD
    A[API Banco Central (IPCA)] -->|Ingestão| B[workspace.bronze_etl.ipca]
    A2[CSV - Boi Gordo] -->|Ingestão| B2[workspace.bronze_etl.boi_gordo]
    B & B2 -->|Transformação + Join| C[workspace.silver_etl.economia]
    C -->|Cálculo de variações percentuais| D[workspace.gold_etl.varicacao_ipca_boiGordo]
```

---

## 🧠 Boas Práticas Implementadas

- ✅ Arquitetura **Medalhão** (Bronze, Silver, Gold)
- ✅ Função **inteligente** `merge_delta_table` com chaves dinâmicas
- ✅ Deduplicação (`dropDuplicates`)
- ✅ Tratamento de `NULL` e divisão segura (`F.coalesce`, `F.when`)
- ✅ Timezone local (`America/Sao_Paulo`)
- ✅ MERGE incremental (sem recriar tabelas)
- ✅ Integração de múltiplas fontes (API + CSV)

---

## 🧾 Autor

**Valter Lafuente Junior**  
💼 Data Engineer 
📅 Projeto: *Pipeline Medalhão Delta Lake (Economia)*  
📍 Stack: *Databricks • PySpark • Delta Lake • GCP*
