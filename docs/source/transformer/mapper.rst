Mapper
======

Class
-----

.. automodule:: spooq2.transformer.mapper

Activity Diagram
---------------------------------------------
# todo: update to new logic

.. uml:: ../diagrams/from_thesis/activity/mapper_1.puml

Available Custom Mapping Methods
--------------------------------------------

as_is / keep / no_change / without_casting (aliases)
***********************************************************************
Only renaming applied. No casting / transformation.

unix_timestamp_ms_to_spark_timestamp
***********************************************************************
unix timestamp in ms (LongType) -> timestamp (TimestampType)

extended_string_to_int
***********************************************************************
Number (IntegerType, FloatType, StringType) -> Number (IntegerType)

extended_string_to_long
***********************************************************************
Number (IntegerType, FloatType, StringType) -> Number (LongType)

extended_string_to_float
***********************************************************************
Number (IntegerType, FloatType, StringType) -> Number (FloatType)

extended_string_to_double
***********************************************************************
Number (IntegerType, FloatType, StringType) -> Number (DoubleType)

extended_string_to_boolean
***********************************************************************
Number (IntegerType, FloatType, StringType, BooleanType) -> boolean (BooleanType)

extended_string_to_timestamp
***********************************************************************
unix timestamp in s or text (IntegerType, FloatType, StringType) -> timestamp (TimestampType)

extended_string_to_date
***********************************************************************
unix timestamp in s or text (IntegerType, FloatType, StringType) -> date (DateType)

extended_string_unix_timestamp_ms_to_timestamp
***********************************************************************
unix timestamp in ms or text (IntegerType, FloatType, StringType) -> timestamp (TimestampType)

extended_string_unix_timestamp_ms_to_date
***********************************************************************
unix timestamp in ms or text (IntegerType, FloatType, StringType) -> date (DateType)

meters_to_cm
***********************************************************************
Number (IntegerType, FloatType, StringType) -> Number * 100 (IntegerType)

has_value
***********************************************************************
Any data -> False if NULL or empty string, otherwise True (BooleanType)

json_string
***********************************************************************
Any input data type will be returned as json (StringType).
Complex data types are supported!

timestamp_ms_to_ms
***********************************************************************
Unix timestamp in ms (LongType) -> Unix timestamp in ms (LongType) if
timestamp is between 1970 and 2099

timestamp_ms_to_s
***********************************************************************
Unix timestamp in ms (LongType) -> Unix timestamp in s (LongType) if
timestamp is between 1970 and 2099

timestamp_s_to_ms
***********************************************************************
Unix timestamp in s (LongType) -> Unix timestamp in ms (LongType) if
timestamp is between 1970 and 2099

timestamp_s_to_s
***********************************************************************
Unix timestamp in s (LongType) -> Unix timestamp in s (LongType) if
timestamp is between 1970 and 2099

StringNull
***********************************************************************
Any data -> NULL (StringType)

IntNull
***********************************************************************
Any data -> NULL (IntegerType)

StringBoolean
***********************************************************************
Any data -> "1" (StringType) if source columns contains any valid data, otherwise NULL

IntBoolean
***********************************************************************
Any data -> 1 (IntegerType) if source columns contains any valid data, otherwise NULL

TimestampMonth
***********************************************************************
Timestamp (TimestampType / DateType) -> 1st day of the input value's month (TimestampType)


Custom Mapping Methods Details
--------------------------------------------

.. automodule:: spooq2.transformer.mapper_custom_data_types
    :ignore-module-all:
    :member-order: bysource
    :private-members:
    :undoc-members:
