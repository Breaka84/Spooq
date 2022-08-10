=========
Changelog
=========

3.4.0 (2022-08-11)
-------------------
* [MOD] Mapper: Add additional parameter allowing skipping of transformations in case the source column is missing:

    - `nullify_missing_columns`: set source column to null in case it does not exist
    - `skip_missing_columns`: skip transformation in case the source column does not exist
    - `ignore_missing_columns`: DEPRECATED -> use `nullify_missing_columns` instead

3.3.7 (2022-03-15)
-------------------
* [FIX] Fix long overflow in extended_string_to_timestamp

3.3.6 (2021-11-19)
-------------------
* [FIX] Fix Cleaners logs in case of field type different than string

3.3.5 (2021-11-16)
-------------------
* [ADD] Add Null Cleaner spooq.transformer.NullCleaner

3.3.4 (2021-07-21)
-------------------
* [MOD] Store null value instead of an empty Map in case no cleansing was necessary

3.3.3 (2021-06-30)
-------------------
* [MOD] Change logic for storing cleansed values as MapType Column to not break Spark (logical plan got to big)
* [MOD] Add streaming tests (parquet & delta) for EnumCleaner unit tests.

3.3.2
-------------------
* Left out intentionally as there is already a yanked version 3.3.2 on PyPi

3.3.1 (2021-06-22)
-------------------
* [MOD] Add option to store logged cleansed values as MapType (Enum & Threshold based cleansers)
* [FIX] Fix TOC on PyPi, add more links to PyPi

3.3.0 (2021-04-22)
-------------------
* [MOD] (BREAKING CHANGE!) rename package back to Spooq (dropping "2").
  This means you need to update all imports from spooq2.xxx.yyy to spooq.xxx.yyy in your code!
* [MOD] Prepare for PyPi release
* [MOD] Drop official support for Spark 2.x (it most probably still works without issues,
  but some tests fail on Spark2 due to different columns ordering and the effort is too high to
  maintain both versions with respect to tests)

3.2.0 (2021-04-13)
-------------------
* [MOD] Add functionality to log cleansed values into separate struct column (column_to_log_cleansed_values)
* [MOD] Add ignore_ambiguous_columns to Mapper
* [MOD] Log spooq version when importing
* [REM] Drop separate spark package (bin-folder) as pip package can now handle all tests as well
* [ADD] Github action to test on label (test-it) or merge into master

3.1.0 (2021-01-27)
-------------------
* [ADD] EnumCleaner Transformer
* [MOD] Add support for dynamic default values with the ThresholdCleaner

3.0.1 (2021-01-22)
-------------------
* [MOD] extended_string_to_timestamp: now keeps milli seconds (no more cast to LongType) for conversion to Timestamp

3.0.0b (2020-12-09)
-------------------
* [ADD] Spark 3 support (different handling in tests via `only_sparkX` decorators)
* [FIX] Fix null types in schema for custom transformations on missing columns
* [MOD] (BREAKING CHANGE!) set default for `ignore_missing_columns` of Mapper to False (fails on missing input columns)

2.3.0 (2020-11-23)
------------------
* [MOD] extended_string_to_timestamp: it can now handle unix timestamps in seconds and in milliseconds
* [MOD] extended_string_to_date: it can now handle unix timestamps in seconds and in milliseconds

2.2.0 (2020-10-02)
------------------
* [MOD] Add support for prepending and appending mappings on input dataframe (Mapper)
* [MOD] Add support for custom spark sql functions in mapper without injecting methods
* [MOD] Add support for "on"/"off" and "enabled"/"disabled" in extended_string_to_boolean custom mapper transformations
* [ADD] New custom mapper transformations:

    - extended_string_to_date
    - extended_string_unix_timestamp_ms_to_date
    - has_value

2.1.1 (2020-09-04)
------------------
* [MOD] `drop_rows_with_empty_array` flag to allow keeping rows with empty array after explosion
* [MOD] Additional test-cases for extended_string mappings (non string inputs)
* [FIX] Remove STDERR logging, don't touch root logging level anymore (needs to be done outside spooq to see some lower log levels)
* [ADD] New custom mapper transformations:

    - extended_string_unix_timestamp_ms_to_timestamp

2.1.0 (2020-08-17)
------------------
* [ADD] Python 3 support
* [MOD] `ignore_missing_columns` flag to fail on missing input columns with Mapper transformer (https://github.com/Breaka84/Spooq/pull/6)
* [MOD] Timestamp support for threshold cleaner
* [ADD] New custom mapper transformations:

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
