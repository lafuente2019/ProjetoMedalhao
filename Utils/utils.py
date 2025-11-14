from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def get_merge_condition(tabela_destino: str) -> str:
    """
    Retorna a condição de MERGE (string SQL) de acordo com a tabela de destino.
    """
    if tabela_destino in [
        "workspace.bronze_etl.ipca",
        "workspace.bronze_etl.boi_gordo",
    ]:
        return "t.data = s.data AND t.valor = s.valor"

    return (
        "t.data = s.data "
        "AND t.ipca = s.ipca "
        "AND t.boi_gordo = s.boi_gordo"
    )


def merge_delta_table(df_join: DataFrame, tabela_destino: str) -> None:
    """
    Realiza MERGE (upsert) de um DataFrame em uma tabela Delta Lake.
    """

    if df_join is None or df_join.limit(1).count() == 0:
        print("⚠️ Nenhum dado para atualizar.")
        return

    total = df_join.count()
    print(f"✅ Total de registros carregados: {total}")

    if spark.catalog.tableExists(tabela_destino):
        print(f"📦 Tabela {tabela_destino} já existe — atualizando dados...")

        delta_table = DeltaTable.forName(spark, tabela_destino)

        condicao_merge = get_merge_condition(tabela_destino)
        print(f"🔑 Condição de merge usada: {condicao_merge}")

        (
            delta_table.alias("t")
            .merge(
                df_join.alias("s"),
                condicao_merge,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"✅ MERGE concluído com sucesso em {tabela_destino}")

    else:
        print(f"🆕 Tabela {tabela_destino} não existe — criando nova tabela...")

        (
            df_join.write.format("delta")
            .partitionBy("data")
            .mode("append")
            .option("overwriteSchema", "true")
            .saveAsTable(tabela_destino)
        )

        print(f"✅ Tabela {tabela_destino} criada e dados inseridos com sucesso.")
