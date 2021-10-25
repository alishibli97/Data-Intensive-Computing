from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("how to read csv file").getOrCreate()

df = spark.read.csv('dataset_with_features.csv',header=True)

df_filtered = df.select(col('danceability'), col('energy'), col('key'), col('loudness'), col('mode'), col('speechiness'), col('acousticness'), col('instrumentalness'), col('liveness'), col('valence'))


df_filtered.show(5)


df_filtered.select(df_filtered['energy']>0.8)

