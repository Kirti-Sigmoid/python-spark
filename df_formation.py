import csv
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.types import *

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

ticker_symbols = ["ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS", "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH",
            "AIN", "AIR", "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX"]

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
file_base_path = '/Users/kirti_sigmoid/PycharmProjects/pythonSparkProject/Data/'

val = 0
new_df = " "

for file in ticker_symbols:

    file_full_path = file_base_path + file + ".csv"
    df = spark.read.csv(file_full_path, header=True,inferSchema=True)
    if val == 0:
        val = 1
        new_df = df.withColumn("stock_name", lit(file))
    else:
        df = df.withColumn("stock_name", lit(file))
        new_df = new_df.union(df)

