from pyspark.sql import SparkSession
from pyspark.sql.functions import *

'''
This pyspark scrip read csv data of electric vehicle population
Remove records with null vin
Replace any nulls values with space
Remove special chars(*#) from all the columns
Write to parquet table.
'''
electric_vehicle_population_data = sys.argv[1]

spark = SparkSession.builder.appName("Pyspark-Electric-Vehicle-Population - Ingestion").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# set up logger using spark logger
log4jLogger = spark._jvm.org.apache.log4j
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

logger.info("pyspark electric vehicle script logger initialized")

logger.info("electric_vehicle_population_data: " + electric_vehicle_population_data)


def write_to_table(output_df, table_name):
    # Check if already exists or not
    # If already exists append the data or create table on the fly

    if spark.catalog.tableExists(table_name):
        output_df.write.mode("append").format("parquet").partitionBy(*["country", "state"]).saveAsTable(table_name)
    else:
        output_df.write.mode("overwrite").format("parquet").partitionBy(*["country", "state"]).saveAsTable(table_name)


def clean_df(df: DataFrame) -> DataFrame:
    # Replace char * and # with space
    for col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[\\*|\\#]", ""))
    return df


def read_csv(in_path: str, delimiter: str, header: str) -> DataFrame:
    # Reading csv with inferschema.
    # Enabling infer schema will do two read operations, which will not be good idea, better to provide schema explicitly
    return spark.read.format("csv") \
        .option("header", header) \
        .option("inferSchema", "true") \
        .option("delimiter", delimiter) \
        .option("quote", '"') \
        .option("escape", '"') \
        .load(in_path)


try:
    in_df = read_csv(electric_vehicle_population_data, ",").filter(col("vin").isNotNull()).na.fill("")
    cleaned_df = clean_df(in_df)
    write_to_table(cleaned_df, "default.electric_vehicle_population")
    spark.stop()
    logger.error("Application succeeded")
except Exception as e:
    logger.error("Application failed with error: " + str(e))
    spark.stop()
    raise e
