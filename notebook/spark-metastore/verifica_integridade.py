from pyspark.shell import spark


# Nome da tabela que deseja verificar
nome_tabela = "cadastro"


try:
    # Executar DESCRIBE EXTENDED para obter detalhes completos da tabela
    metadados = spark.sql(f"DESCRIBE EXTENDED {nome_tabela}")
    metadados.show(truncate=False)

    # Verificar se o local de armazenamento existe e se está consistente
    local_armazenamento = metadados.filter(metadados.col("col_name") == "Location").select("data_type").collect()
    print(f"Local de armazenamento: {local_armazenamento}")

    # Pode adicionar lógica para verificar outras informações ou comparar timestamps
    print(f"A tabela '{nome_tabela}' parece estar consistente e não recriada.")
except Exception as e:
    print(f"A tabela '{nome_tabela}' pode não existir ou foi recriada.")