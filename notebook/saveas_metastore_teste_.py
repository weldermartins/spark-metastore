from pyspark.shell import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Definição do esquema dos dados
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True)
])

# Criando dados fictícios
dados = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Carlos", 35)
]

# Criando DataFrame
df = spark.createDataFrame(dados, schema=schema)

# Salvando como tabela temporária
df.write.mode("overwrite").saveAsTable("pessoas_temp")

# Configurando conexão com PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/itau"
tabela_destino = "public.pessoas"
properties = {
    "user": "postgres",
    "password": "101270",
    "driver": "org.postgresql.Driver"
}

# Lendo a tabela temporária e gravando no PostgreSQL
df_temp = spark.sql("SELECT * FROM pessoas_temp")

df_temp.show()
df_temp.write \
    .jdbc(url=jdbc_url, table=tabela_destino, mode="overwrite", properties=properties)

print("Dados carregados com sucesso!")

# Encerrando a SparkSession
spark.stop()