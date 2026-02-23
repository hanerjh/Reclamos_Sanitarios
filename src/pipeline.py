from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, count

spark = SparkSession.builder \
    .appName("HealthClaimsPipeline") \
    .getOrCreate()

# 1. Cargar datos
patients = spark.read.csv("/Volumes/Workspace/default/reclamos_sanitarios/raw_data/patients.csv", header=True, inferSchema=True)
claims = spark.read.csv("/Volumes/Workspace/default/reclamos_sanitarios/raw_data/claims.csv", header=True, inferSchema=True)
#claims = spark.read.csv("/dbfs/FileStore/data/raw/claims.csv",
                    
# 2. Tokenización
patients = patients.withColumn(
    "patient_token",
    sha2(concat_ws("||", patients['ssn'], patients['birth_date']), 256)
)

# 3. Join para validar reclamos
valid_claims = claims.join(
    patients,
    claims['patient_id'] == patients['patient_id'],
    "inner"
)

# 4. Detectar reclamos huérfanos
orphan_claims = claims.join(
    patients,
    claims['patient_id'] == patients['patient_id'],
    "left_anti"
)

#print("Reclamos huérfanos:", orphan_claims.count())

# 5. Agregación
provider_summary = valid_claims.groupBy("provider_id") \
    .agg(count("*").alias("total_claims"))

# 6. Escritura en Parquet particionado (simulando S3)
valid_claims.write \
    .mode("overwrite") \
    .partitionBy("year_claim", "month-claim") \
    .parquet("/Volumes/Workspace/default/reclamos_sanitarios/data_procesada/claims_parquet")

provider_summary.write \
    .mode("overwrite") \
    .parquet("/Volumes/Workspace/default/reclamos_sanitarios/data_procesada/procesado/provider_summary")

# 7. Explicación del plan
valid_claims.explain()

spark.stop()
