from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

# 1. Iniciar Spark
spark = SparkSession.builder \
    .appName("UNAD_Batch_Limpio") \
    .getOrCreate()

# --- ESTO ES LO QUE CALCA / APAGA LOS MENSAJES MOLESTOS DE INFO ---
sc = spark.sparkContext
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
# -----------------------------------------------------------------

# Cargar archivo
df = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv("Accidentalidad_Caldas_20261028.csv")

print(f"\n>>> TOTAL DE REGISTROS CARGADOS: {df.count()} <<<\n")

print(">>> 1. ACCIDENTES POR CLASE:")
df.groupBy("CLASE").agg(count("*").alias("TOTAL")).orderBy(desc("TOTAL")).show(10, truncate=False)

print(">>> 2. ACCIDENTES POR GRAVEDAD:")
df.groupBy("GRAVEDAD").agg(count("*").alias("TOTAL")).orderBy(desc("TOTAL")).show(10, truncate=False)

print(">>> 3. ACCIDENTES POR DIA DE LA SEMANA:")
df.groupBy("DIA DE LA SEMANA").agg(count("*").alias("TOTAL")).orderBy(desc("TOTAL")).show(10, truncate=False)

spark.stop()

