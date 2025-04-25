from pyspark.shell import spark

# Criar ou carregar um DataFrame
data = [("João", 28, '20250423'), ("Maria", 35, '20250424'), ("Pedro", 40, '20250424')]


columns = ["Nome", "Idade", "anomesdia"]
df = spark.createDataFrame(data, columns)
df = df.coalesce(1)

# Salvar o DataFrame no Metastore como uma tabela gerenciada
df.write.mode("overwrite").partitionBy('anomesdia').saveAsTable("cadastro")


df1 = spark.sql('select * from cadastro')
df1.show()
