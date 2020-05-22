from random import randint
from pyspark.sql import functions as F

SCHEMA_VERSION = "1"
INPUT_FILE_NAME = "schema_v{nr}.json".format(nr=SCHEMA_VERSION)
DATE_COLUMNS_TO_CONVERT = [("attributes.birthday", "birthday")]

rdd_text = spark.sparkContext.textFile(INPUT_FILE_NAME)
rdd_text.coalesce(2).saveAsTextFile(
    "schema_v{nr}/textFiles".format(nr=SCHEMA_VERSION))

rdd_seq = rdd_text.map(lambda x:
                       (randint(1000, 100000), bytearray(x, "utf-8")))
rdd_seq.coalesce(2).saveAsSequenceFile(
    "schema_v{nr}/sequenceFiles".format(nr=SCHEMA_VERSION))

df = spark.read.json(rdd_text)
for input_col, output_col in DATE_COLUMNS_TO_CONVERT:
    df = df.withColumn(
        output_col,
        F.to_utc_timestamp(timestamp=F.to_timestamp(df[input_col]),
                           tz="Europe/Vienna"))
df.coalesce(2).write.parquet(
    "schema_v{nr}/parquetFiles".format(nr=SCHEMA_VERSION))
