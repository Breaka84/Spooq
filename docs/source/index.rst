.. Spooq documentation master file, created by
   sphinx-quickstart on Tue Jul 31 21:34:32 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Spooq's documentation!
=================================

Spooq is your **PySpark** based helper library for ETL data ingestion pipeline in Data Lakes.

The main components are:
* Extractors
* Transformers
* Loaders

Those components are independent and can be used separately or be plugged-in into a pipeline instance.
You can also use the custom functions from the Mapper transformer directly with PySpark (f.e. ``select`` or ``withColumn``).

Example of Mapper Transformer
=============================

.. code-block:: python

   from pyspark.sql import Row
   from pyspark.sql import functions as F, types as T
   from spooq.transformer import Mapper
   from spooq.transformer import mapper_transformations as spq

   input_df = spark.createDataFrame(
      [
         Row(
            struct_a=Row(idx="000_123_456", sts="enabled", ts="1597069446000"),
            struct_b=Row(itms="1,2,4", sts="whitelisted", ts="2020-08-12T12:43:14+0000"),
            struct_c=Row(email="abc@def.com", gndr="F", dt="2020-08-05", cmt="fine"),
         ),
         Row(
            struct_a=Row(idx="000_654_321", sts="off", ts="1597069500784"),
            struct_b=Row(itms="5", sts="blacklisted", ts="2020-07-01T12:43:14+0000"),
            struct_c=Row(email="", gndr="m", dt="2020-06-27", cmt="faulty"),
         ),
      ],
      schema="""
         a: struct<idx string, sts string, ts string>,
         b: struct<itms string, sts string, ts string>,
         c: struct<email string, gndr string, dt string, cmt string>
      """
   )
   input_df.printSchema()
   root
    |-- a: struct (nullable = true)
    |    |-- idx: string (nullable = true)
    |    |-- sts: string (nullable = true)
    |    |-- ts: string (nullable = true)
    |-- b: struct (nullable = true)
    |    |-- itms: string (nullable = true)
    |    |-- sts: string (nullable = true)
    |    |-- ts: string (nullable = true)
    |-- c: struct (nullable = true)
    |    |-- email: string (nullable = true)
    |    |-- gndr: string (nullable = true)
    |    |-- dt: string (nullable = true)
    |    |-- cmt: string (nullable = true)


   mapping = [
       # output_name     # source                # transformation
      ("index",          "a.idx",                spq.to_int),  # removes leading zeros and underline characters
      ("is_enabled",     "a.sts",                spq.to_bool),  # recognizes additional words like "on", "off", "disabled", "enabled", ...
      ("a_updated_at",   "a.ts",                 spq.to_timestamp),  # supports unix timestamps in ms or seconds and strings
      ("items",          "b.itms",               spq.str_to_array(cast="int")),  # splits a comma delimited string into an array and casts its elements
      ("block_status",   "b.sts",                spq.map_values(mapping={"whitelisted": "allowed", "blacklisted": "blocked"})),  # applies lookup dictionary
      ("b_updated_at",   "b.ts",                 spq.to_timestamp),  # supports unix timestamps in ms or seconds and strings
      ("has_email",      "c.email",              spq.has_value),  # interprets also empty strings as no value, although, zeros are values
      ("gender",         "c.gndr",               spq.apply(func=F.lower)),  # applies provided function to all values
      ("creation_date",  "c.dt",                 spq.to_timestamp(cast="date")),  # explicitly casts result after transformation
      ("processed_at",   F.current_timestamp(),  spq.as_is),  # source column is a function, no transformation to the results
      ("comment",        "c.cmt",                "string"),  # no transformation, only cast; alternatively: spq.to_str or spq.as_is(cast="string")
   ]
   output_df = Mapper(mapping).transform(input_df)

   output_df.show(truncate=False)
   +------+----------+-----------------------+---------+------------+-------------------+---------+------+-------------+----------------------+-------+
   |index |is_enabled|a_updated_at           |items    |block_status|b_updated_at       |has_email|gender|creation_date|processed_at          |comment|
   +------+----------+-----------------------+---------+------------+-------------------+---------+------+-------------+----------------------+-------+
   |123456|true      |2020-08-10 16:24:06    |[1, 2, 4]|allowed     |2020-08-12 14:43:14|true     |f     |2020-08-05   |2022-08-12 09:17:09.83|fine   |
   |654321|false     |2020-08-10 16:25:00.784|[5]      |blocked     |2020-07-01 14:43:14|false    |m     |2020-06-27   |2022-08-12 09:17:09.83|faulty |
   +------+----------+-----------------------+---------+------------+-------------------+---------+------+-------------+----------------------+-------+


   output_df.printSchema()
   root
    |-- index: integer (nullable = true)
    |-- is_enabled: boolean (nullable = true)
    |-- a_updated_at: timestamp (nullable = true)
    |-- items: array (nullable = true)
    |    |-- element: integer (containsNull = true)
    |-- block_status: string (nullable = true)
    |-- b_updated_at: timestamp (nullable = true)
    |-- has_email: boolean (nullable = false)
    |-- gender: string (nullable = true)
    |-- creation_date: date (nullable = true)
    |-- processed_at: timestamp (nullable = false)
    |-- comment: string (nullable = true)


Table of Content
================
.. toctree::
    :maxdepth: 3
    :name: contents

    installation
    get_started
    components
    contribute
    changelog

Indices and tables
==================

* :ref:`modindex`
* :ref:`search`
