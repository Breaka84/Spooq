=========
Changelog
=========

3.2.0 (2021-04-13)
-------------------
* [MOD] add functionality to log cleansed values into separate struct column (column_to_log_cleansed_values)
* [MOD] add ignore_ambiguous_columns to Mapper
* [MOD] log spooq version when importing
* [REM] Drop separate spark package (bin-folder) as pip package can now handle all tests as well
* [ADD] Github action to test on label (test-it) or merge into master

3.1.0 (2021-01-27)
-------------------
* [ADD] EnumCleaner Transformer
* [MOD] add support for dynamic default values with the ThresholdCleaner

3.0.1 (2021-01-22)
-------------------
* [MOD] extended_string_to_timestamp: now keeps milli seconds (no more cast to LongType) for conversion to Timestamp

3.0.0b (2020-12-09)
-------------------
* [ADD] Spark 3 support (different handling in tests via `only_sparkX` decorators)
* [FIX] fix null types in schema for custom transformations on missing columns
* [MOD] (BREAKING CHANGE!) set default for `ignore_missing_columns` of Mapper to False (fails on missing input columns)

2.3.0 (2020-11-23)
------------------
* [MOD] extended_string_to_timestamp: it can now handle unix timestamps in seconds and in milliseconds
* [MOD] extended_string_to_date: it can now handle unix timestamps in seconds and in milliseconds

2.2.0 (2020-10-02)
------------------
* [MOD] add support for prepending and appending mappings on input dataframe (Mapper)
* [MOD] add support for custom spark sql functions in mapper without injecting methods
* [MOD] add support for "on"/"off" and "enabled"/"disabled" in extended_string_to_boolean custom mapper transformations
* [ADD] new custom mapper transformations:

    - extended_string_to_date
    - extended_string_unix_timestamp_ms_to_date
    - has_value

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
