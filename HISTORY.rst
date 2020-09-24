=======
History
=======
2.2.0 (2020-09-24)
------------------
* [MOD] add support for prepending and appending mappings on input dataframe (Mapper)
* [MOD] add support for custom spark sql functions in mapper without injecting methods
* [MOD] add support for "on"/"off" and "enabled"/"disabled" in extended_string_to_boolean custom mapper transformations

2.1.1 (2020-09-04)
------------------
* [MOD] `drop_rows_with_empty_array` flag to allow keeping rows with empty array after explosion
* [MOD] additional test-cases for extended_string mappings (non string inputs)
* [FIX] remove STDERR logging, don't touch root logging level anymore (needs to be done outside spooq to see some lower log levels)
* [ADD] new custom mapper transformations:
    - extended_string_unix_timestamp_ms_to_timestamp

2.1.0 (2020-08-17)
------------------
* [ADD] Python 3 support
* [MOD] `ignore_missing_columns` flag to fail on missing input columns with Mapper transformer (https://github.com/Breaka84/Spooq/pull/6)
* [MOD] timestamp support for threshold cleaner
* [ADD] new custom mapper transformations:
    - meters_to_cm
    - unix_timestamp_ms_to_spark_timestamp
    - extended_string_to_int
    - extended_string_to_long
    - extended_string_to_float
    - extended_string_to_double
    - extended_string_to_boolean
    - extended_string_to_timestamp

2.0.0 (2020-05-22)
------------------
* [UPDATE] Upgrade to use Spark 2 (tested for 2.4.3) -> will no longer work for spark 1
* Breaking changes (severe refactoring)


0.6.2 (2019-05-13)
------------------
* [FIX] Logger writes now to std_out and std_err & logger instance is shared across all spooq instances
* [FIX] PyTest version locked to 3.10.1 as 4+ broke the tests
* [MOD] Removes id_function to create names for parameters in test methods (fallback to built-in)
* [ADD] Change SelectNewestByGroup from string eval to pyspark objects
* [FIX] json_string is now able to None values


0.6.1 (2019-03-26)
------------------
* [FIX] PassThrough Extractor (input df now defined at instantiation time)
* [ADD] json_string new custom data type
