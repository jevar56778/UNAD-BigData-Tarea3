from pyspark.sql import SparkSession

# Creamos una sesión con un nombre diferente para evitar conflictos
spark = SparkSession.builder \
    .appName("UNAD_Streaming_Accidentalidad") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Conectarnos al mismo Kafka pero escuchando el tópico de accidentalidad
df_streaming = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "streaming-accidentalidad") \
    .load()

df_mensajes = df_streaming.selectExpr("CAST(value AS STRING) as mensaje")

query = df_mensajes.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
