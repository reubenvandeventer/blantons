from pyspark.sql import SparkSession
import sys
import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import configparser
import sys
import traceback
### add your input file path with file name
file_in = 'input.txt'
driver_memory='20g'

try:
    start_time=str(datetime.datetime.now())
    print ("start time =",str(datetime.datetime.now()))
    spark = SparkSession.builder.appName("Incremental Application").config("spark.some.config.option", "some-value").config("spark.driver.memory", driver_memory).getOrCreate()
    print("session start")

    current_dataset = spark.read.format("csv").option("header", "true").option("delimiter", ',').load(file_in)

#### need to provide column name
    df = current_dataset.select("v6392", "v0223", "v0020", "v0026", "v0027", "v0157",
    "v0165", "v0354", "v0628", "v0881", "v1673", "v2028", "v2484", "v3086", "v3947",
    "v3950", "v4939", "v4940", "v4944", "v4958", "v4960", "v5057", "v5066", "v5076",
    "v5080", "v5089", "v5098", "v5245", "v5264", "v5296", "v5306", "v5330", "v5339",
    "v5736", "v5740", "v5812", "v6296", "v6302", "v6353", "v6362", "v6367", "v6372",
    "v6379", "v6400")

#### need to change output path here
    df.coalesce(1).write.option("sep",',').mode('overwrite')\
    .option("header","true")\
    .csv("/Users/reubenvandeventer/PycharmProjects/ruffian/output")
except Exception as e:
    print(e)


