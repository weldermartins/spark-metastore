from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Inicializando a Glue SparkSession
spark = SparkSession.builder \
    .appName("AWS Glue SaveAs & InsertInto") \
    .enableHiveSupport() \
    .getOrCreate()

# Criando esquema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True)
])

# Criando DataFrame fictício
dados = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Carlos", 35)
]
df = spark.createDataFrame(dados, schema=schema)

# Salvando como tabela no AWS Glue (equivalente ao PostgreSQL temporário)
df.write.mode("overwrite").saveAsTable("glue_db.pessoas_temp")

print("Tabela temporária criada no AWS Glue!")

# Lendo a tabela temporária
df_temp = spark.sql("SELECT * FROM glue_db.pessoas_temp")

# Inserindo os dados na tabela produtiva
df_temp.write.mode("append").insertInto("glue_db.pessoas_produtiva")

print("Dados inseridos na tabela produtiva!")

# Encerrando Spark
spark.stop()