from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("pyspark")

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .load("resources/csv/test.csv")

df.show(5, False)
