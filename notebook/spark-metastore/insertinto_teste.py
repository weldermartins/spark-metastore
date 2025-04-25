from pyspark.shell import spark

# Criar ou carregar um DataFrame com os dados
data = data = [("kiara", 5, '20250422')]
columns = ["Nome", "Idade"]
df = spark.createDataFrame(data, columns)
df = df.coalesce(1)
# Nome da tabela existente no Metastore
nome_tabela = "cadastro"

# Inserir os dados na tabela usando insertInto
(df.write
    .mode("append")
    .insertInto(nome_tabela))


df1 = spark.sql('select * from cadastro')
df1.show()
