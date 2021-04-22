[![Documentation Status](https://readthedocs.org/projects/spooq/badge/?version=latest)](https://spooq.readthedocs.io/en/latest/?badge=latest)

# Welcome to Spooq

**Spooq** is a PySpark based helper library for ETL data ingestion pipeline in Data Lakes.   
The main components are:
* Extractors  
* Transformers  
* Loaders  

Those components can be plugged-in into a pipeline instance or used separately.

# Table of Content

<!-- TOC -->

- [Welcome to Spooq](#welcome-to-spooq)
- [Table of Content](#table-of-content)
- [Features / Components](#features--components)
- [Installation](#installation)
- [Examples](#examples)
    - [Pipeline with JSON source, various transformations and HIVE target database](#pipeline-with-json-source-various-transformations-and-hive-target-database)
    - [Structured Streaming with selected Transformers](#structured-streaming-with-selected-transformers)
- [Online Documentation](#online-documentation)
- [Changelog](#changelog)
- [Contribution](#contribution)
- [License](#license)

<!-- /TOC -->

# Features / Components

* [**Extractors**](https://spooq.readthedocs.io/en/latest/extractor/overview.html)
    * [JSON Files](https://spooq.readthedocs.io/en/latest/extractor/json.html)
    * [JDBC Source](https://spooq.readthedocs.io/en/latest/extractor/jdbc.html)
* [**Transformers**](https://spooq.readthedocs.io/en/latest/transformer/overview.html)
    * [Exploder](https://spooq.readthedocs.io/en/latest/transformer/exploder.html)
    * [Filter](https://spooq.readthedocs.io/en/latest/transformer/sieve.html)
    * [Mapper (Restructuring of complex DataFrames)](https://spooq.readthedocs.io/en/latest/transformer/mapper.html)
    * [Threshold-based Cleanser](https://spooq.readthedocs.io/en/latest/transformer/threshold_cleaner.html)
    * [Enumeration-based Cleanser](https://spooq.readthedocs.io/en/latest/transformer/enum_cleaner.html)
    * [Newest by Group (Most current record per ID)](https://spooq.readthedocs.io/en/latest/transformer/newest_by_group.html)
* [**Loaders**](https://spooq.readthedocs.io/en/latest/loader/overview.html)
    * [Hive Database](https://spooq.readthedocs.io/en/latest/loader/hive_loader.html)

# Installation

```bash
pip install spooq
```

# Examples

## Pipeline with JSON source, various transformations and HIVE target database
```python
from pyspark.sql import functions as F
import datetime
from spooq.pipeline import Pipeline
import spooq.extractor as   E
import spooq.transformer as T
import spooq.loader as      L

# Schema of input data
# 
#  |-- attributes: struct (nullable = true)
#  |    |-- birthday: string (nullable = true)
#  |    |-- email: string (nullable = true)
#  |    |-- first_name: string (nullable = true)
#  |    |-- friends: array (nullable = true)
#  |    |    |-- element: struct (containsNull = true)
#  |    |    |    |-- first_name: string (nullable = true)
#  |    |    |    |-- id: long (nullable = true)
#  |    |    |    |-- last_name: string (nullable = true)
#  |    |-- gender: string (nullable = true)
#  |    |-- ip_address: string (nullable = true)
#  |    |-- last_name: string (nullable = true)
#  |    |-- university: string (nullable = true)
#  |-- guid: string (nullable = true)
#  |-- id: long (nullable = true)
#  |-- location: struct (nullable = true)
#  |    |-- latitude: string (nullable = true)
#  |    |-- longitude: string (nullable = true)
#  |-- meta: struct (nullable = true)
#  |    |-- created_at_ms: long (nullable = true)
#  |    |-- created_at_sec: long (nullable = true)
#  |    |-- version: long (nullable = true)

#  Definition how the output table should look like and where the attributes come from:
partition_value = F.lit(datetime.date.today().strftime("%Y%m%d"))
users_mapping = [
    ("id",              "id",                                "IntegerType"),
    ("guid",            "guid",                              "StringType"),
    ("forename",        "attributes.first_name",             "StringType"),
    ("surename",        "attributes.last_name",              "StringType"),
    ("gender",          F.upper(F.col("attributes.gender")), "StringType"),
    ("has_email",       "attributes.email",                  "has_value"),
    ("has_university",  "attributes.university",             "has_value"),
    ("created_at",      "meta.created_at_ms",                "extended_string_to_timestamp"),
    ("dt",              partition_value,                     "IntegerType"),
]

#  The main object where all steps are defined:
users_pipeline = Pipeline()

#  Defining the EXTRACTION (SequenceFiles with timestamp & json string as bytecode):
users_pipeline.set_extractor(E.JSONExtractor(
    input_path="tests/data/schema_v1/sequenceFiles"
))

#  Defining the TRANSFORMATIONS:
users_pipeline.add_transformers([
    T.Mapper(mapping=users_mapping),
    T.ThresholdCleaner(
        thresholds={
            "created_at": {"min": 0, "max": 1580737513, "default": None}
        },
        column_to_log_cleansed_values="cleansed_values_threshold"),
    T.EnumCleaner(
        cleaning_definitions={
            "gender": {
                "elements": ["F", "M", "X"], 
                "mode": "allow", 
                "default": "undefined"
            }
        },
        column_to_log_cleansed_values="cleansed_values_enum"
    ),
    T.NewestByGroup(group_by="id", order_by="created_at")
])

#  Defining the LOADING:
users_pipeline.set_loader(L.HiveLoader(
    db_name="users_and_friends",
    table_name="users",
    partition_definitions=[{
        "column_name": "dt",
        "column_type": "IntegerType",
        "default_value": -1}],
    repartition_size=10,
))

#  Executing the whole ETL pipeline
users_pipeline.execute()
```

## Structured Streaming with selected Transformers

```python
from pyspark.sql import functions as F
import datetime
from spooq.transformer import Mapper

# Input Stream
input_stream = (
    spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

#  Definition how the output stream should look like and where the attributes come from:
partition_value = F.lit(datetime.date.today().strftime("%Y%m%d"))
users_mapping = [
    ("id",              "id",                                "IntegerType"),
    ("guid",            "guid",                              "StringType"),
    ("forename",        "attributes.first_name",             "StringType"),
    ("surename",        "attributes.last_name",              "StringType"),
    ("gender",          F.upper(F.col("attributes.gender")), "StringType"),
    ("has_email",       "attributes.email",                  "has_value"),
    ("has_university",  "attributes.university",             "has_value"),
    ("created_at",      "meta.created_at_ms",                "timestamp_ms_to_s"),
    ("dt",              partition_value,                     "IntegerType"),
]
mapped_stream = Mapper(mapping=users_mapping).transform(input_stream)

# Cleansing gender attribute
cleansed_stream = EnumCleaner(
    cleaning_definitions={
        "gender": {
            "elements": ["F", "M", "X"], 
            "mode": "allow", 
            "default": "undefined"
        }
    },
    column_to_log_cleansed_values="cleansed_values_enum"
).transform(mapped_stream)

# Output Stream
output_stream = (
    cleansed_stream
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

output_stream.awaitTermination()
```

# Online Documentation

For a more details please consult the online documentation at [spooq.readthedocs.io](https://spooq.readthedocs.io/).

# Changelog

[Changelog](https://spooq.readthedocs.io/en/latest/changelog.html)

# Contribution

Please see [CONTRIBUTING.rst](CONTRIBUTING.rst) for more information.

# License

This library is licensed under the [MIT License](!LICENSE).
