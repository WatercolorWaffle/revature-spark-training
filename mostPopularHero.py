from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('Superheroes').getOrCreate()

schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType())
])

names = spark.read.schema(schema).option('sep', ' ').csv('./MarvelNames.txt')
lines = spark.read.text('./MarvelGraph.txt')

connections = lines.withColumn('id', F.split(F.col('value'), ' ')[0]) \
.withColumn('connections', F.size(F.split(F.col('value'), ' ')) - 1) \
.groupBy('id').agg(F.sum('connections').alias('connections'))

mostPopular = connections.sort(F.col('connections').desc()).first()

print(mostPopularName[0] + '')

spark.stop()