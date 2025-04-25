from pyspark.shell import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType



# Definição do esquema dos dados
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True)
])

# Criando um DataFrame com dados fictícios
dados = [
    (3, "maya", 2),


]

df = spark.createDataFrame(dados, schema=schema)

# Salvando a tabela temporária no PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/itau"
tabela_temp = "public.pessoas_temp"
properties = {
    "user": "postgres",
    "password": "101270",
    "driver": "org.postgresql.Driver"
}

# Salvando a tabela temporária
df.write \
    .jdbc(url=jdbc_url, table=tabela_temp, mode="overwrite", properties=properties)

print("Tabela temporária criada com sucesso!")

# Lendo os dados da tabela temporária no PostgreSQL
df_temp = spark.read \
    .jdbc(url=jdbc_url, table=tabela_temp, properties=properties)

# Preparando os dados para inserção na tabela produtiva
tabela_produtiva = "public.pessoas_produtiva"

# Inserindo os dados sem recriar a tabela
df_temp.write \
    .mode("append") \
    .jdbc(url=jdbc_url, table=tabela_produtiva, properties=properties)

print("Dados inseridos na tabela produtiva sem recriação!")

# Encerrando a SparkSession
spark.stop()