import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T

complex_event_expression = (
        F.when(F.col("nested.input_key_1").isNotNull(), F.col("nested.input_key_1") / 1000)
            .otherwise(F.col("nested.input_key_2") / 1000)
            .cast(T.TimestampType())
            .cast(T.DateType())
    )

# fmt: off
fixtures_for_spark_sql_object = [
    # input_value_1           # input_value_1        # mapper function                   # expected_value
    ("place_holder",          "place_holder",        F.current_date(),                   datetime.date.today()),
    ("place_holder",          "place_holder",        F.current_timestamp(),              datetime.datetime.now()),
    ("some string",           "place_holder",        F.col("nested.input_key_1"),        "some string"),
    ("some string to count",  "place_holder",        F.length("nested.input_key_1"),     20),
    ("some string",           None,                  F.coalesce("nested.input_key_1",
                                                                "nested.input_key_2"),   "some string"),
    (None,                    "some other string",   F.coalesce("nested.input_key_1",
                                                                "nested.input_key_2"),   "some other string"),
    (1597069446,              "placeholder",         (F.col("nested.input_key_1")
                                                      .cast(T.TimestampType())
                                                      .cast(T.DateType())),              datetime.date(2020, 8, 10)),
    (1597069446000,             None,                complex_event_expression,           datetime.date(2020, 8, 10)),
    (None,                      1597069446000,       complex_event_expression,           datetime.date(2020, 8, 10)),
    (1597242246000,             1597069446000,       complex_event_expression,           datetime.date(2020, 8, 12)),
]

fixtures_for_has_value = [
    ("1",        True),
    ("",         False),
    ("1.0",      True),
    ("TRUE",     True),
    ("True",     True),
    ("true",     True),
    ("0",        True),
    ("0.0",      True),
    ("-1",       True),
    ("-1.0",     True),
    ("123",      True),
    ("-123",     True),
    (None,       False),
    ("FALSE",    True),
    ("False",    True),
    ("false",    True),
    ("y",        True),
    ("n",        True),
    ("yes",      True),
    ("no",       True),
    ("enabled",  True),
    ("disabled", True),
    ("on",       True),
    ("off",      True),
    ("   true",  True),
    ("true   ",  True),
    (" true  ",  True),
    (1,          True),
    (0,          True),
    (-1,         True),
    (1.0,        True),
    (0.0,        True),
    (-1.0,       True),
    (True,       True),
    (False,      True),
]

fixtures_for_extended_string_to_int = [
    ("123456",        123456),
    ("-123456",      -123456),
    ("-1",                -1),
    ("0",                  0),
    ("NULL",            None),
    ("null",            None),
    ("None",            None),
    (None,              None),
    ("Hello World",     None),
    ("2020-08-12",      None),
    ("1234.56",         1234),
    ("-1234.56",       -1234),
    ("123,456",         None),  # commas not allowed due to their ambiguity
    ("-123,456",        None),  # commas not allowed due to their ambiguity
    ("123_456",       123456),
    ("-123_456",     -123456),
    ("   123456",     123456),
    ("123456   ",     123456),
    (" 123456  ",     123456),
    (123456,          123456),
    (-123456,        -123456),
    (-1,                  -1),
    (0,                    0),
    (1234.56,           1234),
    (-1234.56,         -1234),
]

fixtures_for_extended_string_to_long = [
    ("21474836470",                21474836470),
    ("-21474836470",              -21474836470),
    ("-1",                                  -1),
    ("0",                                    0),
    ("NULL",                              None),
    ("null",                              None),
    ("None",                              None),
    (None,                                None),
    ("Hello World",                       None),
    ("2020-08-12T12:43:14+0000",          None),
    ("214748364.70",                 214748364),
    ("-214748364.70",               -214748364),
    ("21,474,836,470",                    None),  # commas not allowed due to their ambiguity
    ("-21,474,836,470",                   None),  # commas not allowed due to their ambiguity
    ("214748364,7",                       None),  # commas not allowed due to their ambiguity
    ("-214748364,7",                      None),  # commas not allowed due to their ambiguity
    ("21_474_836_470",             21474836470),
    ("-21_474_836_470",           -21474836470),
    ("   21474836470",             21474836470),
    ("21474836470   ",             21474836470),
    (" 21474836470  ",             21474836470),
    (2147483.64,                       2147483),
    (-2147483.64,                     -2147483),
    (21474836470,                  21474836470),
    (-21474836470,                -21474836470),
    (-1,                                    -1),
    (0,                                      0),
]

fixtures_for_extended_string_to_float = [
    ("123456.0",      123456.0),
    ("123456",        123456.0),
    ("-123456.0",    -123456.0),
    ("-1.0",              -1.0),
    ("0.0",                0.0),
    ("NULL",              None),
    ("null",              None),
    ("None",              None),
    (None,                None),
    ("1234.56",        1234.56),
    ("-1234.56",      -1234.56),
    ("-123,456.7",        None),  # commas not allowed due to their ambiguity
    ("123,456.7",         None),  # commas not allowed due to their ambiguity
    ("-123456,7",         None),  # commas not allowed due to their ambiguity
    ("123456,7",          None),  # commas not allowed due to their ambiguity
    ("123_456.7",     123456.7),
    ("-123_456.7",   -123456.7),
    ("   123456.7",   123456.7),
    ("123456.7   ",   123456.7),
    (" 123456.7  ",   123456.7),
    (123456.0,        123456.0),
    (123456,          123456.0),
    (-123456.0,      -123456.0),
    (-1.0,                -1.0),
    (0.0,                  0.0),
    ("Hello",             None),
    ("2k",                None),
]

fixtures_for_extended_string_to_double = [
    ("214748364.70",          214748364.70),
    ("214748364",              214748364.0),
    ("-214748364.70",        -214748364.70),
    ("-1.0",                          -1.0),
    ("0.0",                            0.0),
    ("NULL",                          None),
    ("null",                          None),
    ("None",                          None),
    (None,                            None),
    ("21474836470",          21474836470.0),
    ("-21474836470",        -21474836470.0),
    ("21,474,836,470.7",              None),  # commas not allowed due to their ambiguity
    ("-21,474,836,470.7",             None),  # commas not allowed due to their ambiguity
    ("21474836470,7",                 None),  # commas not allowed due to their ambiguity
    ("-21474836470,7",                None),  # commas not allowed due to their ambiguity
    ("21_474_836_470.7",     21474836470.7),
    ("-21_474_836_470.7",   -21474836470.7),
    ("   21474836470.7",     21474836470.7),
    ("21474836470.7   ",     21474836470.7),
    (  " 21474836470.7  ",   21474836470.7),
    (214748364.70,            214748364.70),
    (214748364,                214748364.0),
    (-214748364.70,          -214748364.70),
    (-1.0,                            -1.0),
    (0.0,                              0.0),
    ("Hello",                         None),
    ("2k",                            None),
]

fixtures_for_extended_string_to_boolean = [
    ("1",        True),
    ("1.0",      None),
    ("TRUE",     True),
    ("True",     True),
    ("true",     True),
    ("0",        False),
    ("0.0",      None),
    ("-1",       None),
    ("-1.0",     None),
    ("123",      None),
    ("-123",     None),
    (None,       None),
    ("FALSE",    False),
    ("False",    False),
    ("false",    False),
    ("y",        True),
    ("n",        False),
    ("yes",      True),
    ("no",       False),
    ("enabled",  True),
    ("disabled", False),
    ("on",       True),
    ("off",      False),
    ("   true",  True),
    ("true   ",  True),
    (" true  ",  True),
    (1,          True),
    (0,          False),
    (-1,         None),
    (1.0,        None),
    (0.0,        None),
    (-1.0,       None),
    (True,       True),
    (False,      False),
]

fixtures_for_extended_string_to_timestamp_spark2 = [
    ("2020-08-12T12:43:14+0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("  2020-08-12T12:43:14+0000", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+0000  ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    (" 2020-08-12T12:43:14+0000 ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+02:00",  datetime.datetime(2020, 8, 12, 10, 43, 14)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12 12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12",                 datetime.datetime(2020, 8, 12, 0, 0, 0)),
    (None,                         None),
    ("1597069446",                 datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446,                   datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446",                datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446,                  datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("1597069446000",              datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446000,                datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446000",             datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446000,               datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("null",                       None),
    ("0",                          datetime.datetime(1970, 1, 1, 0, 0, 0)),
    ("-1",                         datetime.datetime(1969, 12, 31, 23, 59, 59)),
    ("1",                          datetime.datetime(1970, 1, 1, 0, 0, 1)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]

fixtures_for_extended_string_to_timestamp = [
    ("2020-08-12T12:43:14+0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z00:00",  None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("2020-08-12T12:43:14Z0000",   None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("  2020-08-12T12:43:14+0000", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+0000  ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    (" 2020-08-12T12:43:14+0000 ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+02:00",  datetime.datetime(2020, 8, 12, 10, 43, 14)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12 12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12",                 datetime.datetime(2020, 8, 12, 0, 0, 0)),
    (None,                         None),
    ("1597069446",                 datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446,                   datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446",                datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446,                  datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("1597069446000",              datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446000,                datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446000",             datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446000,               datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("null",                       None),
    ("0",                          datetime.datetime(1970, 1, 1, 0, 0, 0)),
    ("-1",                         datetime.datetime(1969, 12, 31, 23, 59, 59)),
    ("1",                          datetime.datetime(1970, 1, 1, 0, 0, 1)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]

fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2 = [
    ("2020-08-12T12:43:14+0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("  2020-08-12T12:43:14+0000", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+0000  ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    (" 2020-08-12T12:43:14+0000 ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+02:00",  datetime.datetime(2020, 8, 12, 10, 43, 14)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12 12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12",                 datetime.datetime(2020, 8, 12, 0, 0, 0)),
    (None,                         None),
    ("1597069446000",              datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446000,                datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446000",             datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446000,               datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("null",                       None),
    ("0",                          datetime.datetime(1970, 1, 1, 0, 0, 0)),
    ("-1",                         datetime.datetime(1969, 12, 31, 23, 59, 59)),
    ("1",                          datetime.datetime(1970, 1, 1, 0, 0, 1)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]

fixtures_for_extended_string_unix_timestamp_ms_to_timestamp = [
    ("2020-08-12T12:43:14+0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z00:00",  None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("2020-08-12T12:43:14Z0000",   None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("  2020-08-12T12:43:14+0000", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+0000  ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    (" 2020-08-12T12:43:14+0000 ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+02:00",  datetime.datetime(2020, 8, 12, 10, 43, 14)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12 12:43:14",        datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12",                 datetime.datetime(2020, 8, 12, 0, 0, 0)),
    (None,                         None),
    ("1597069446000",              datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446000,                datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446000",             datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446000,               datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("null",                       None),
    ("0",                          datetime.datetime(1970, 1, 1, 0, 0, 0)),
    ("-1",                         datetime.datetime(1969, 12, 31, 23, 59, 59)),
    ("1",                          datetime.datetime(1970, 1, 1, 0, 0, 1)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]

fixtures_for_extended_string_to_date_spark2 = [
    ("2020-08-12T12:43:14+0000",   datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z0000",   datetime.date(2020, 8, 12)),
    ("  2020-08-12T12:43:14+0000", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0000  ", datetime.date(2020, 8, 12)),
    (" 2020-08-12T12:43:14+0000 ", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+02:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12 12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12",                 datetime.date(2020, 8, 12)),
    (None,                         None),
    ("1597069446",                 datetime.date(2020, 8, 10)),
    (1597069446,                   datetime.date(2020, 8, 10)),
    ("-1597069446",                datetime.date(1919, 5, 24)),
    (-1597069446,                  datetime.date(1919, 5, 24)),
    ("1597069446000",              datetime.date(2020, 8, 10)),
    ("-1597069446000",             datetime.date(1919, 5, 24)),
    ("null",                       None),
    ("0",                          datetime.date(1970, 1, 1,)),
    ("-1",                         datetime.date(1969, 12, 31,)),
    ("1",                          datetime.date(1970, 1, 1,)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]

fixtures_for_extended_string_to_date = [
    ("2020-08-12T12:43:14+0000",   datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z00:00",  None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("2020-08-12T12:43:14Z0000",   None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("  2020-08-12T12:43:14+0000", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0000  ", datetime.date(2020, 8, 12)),
    (" 2020-08-12T12:43:14+0000 ", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+02:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12 12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12",                 datetime.date(2020, 8, 12)),
    (None,                         None),
    ("1597069446",                 datetime.date(2020, 8, 10)),
    (1597069446,                   datetime.date(2020, 8, 10)),
    ("-1597069446",                datetime.date(1919, 5, 24)),
    (-1597069446,                  datetime.date(1919, 5, 24)),
    ("1597069446000",              datetime.date(2020, 8, 10)),
    ("-1597069446000",             datetime.date(1919, 5, 24)),
    ("null",                       None),
    ("0",                          datetime.date(1970, 1, 1,)),
    ("-1",                         datetime.date(1969, 12, 31,)),
    ("1",                          datetime.date(1970, 1, 1,)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]

fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2 = [
    ("2020-08-12T12:43:14+0000",   datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z0000",   datetime.date(2020, 8, 12)),
    ("  2020-08-12T12:43:14+0000", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0000  ", datetime.date(2020, 8, 12)),
    (" 2020-08-12T12:43:14+0000 ", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+02:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12 12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12",                 datetime.date(2020, 8, 12)),
    (None,                         None),
    ("1597069446000",              datetime.date(2020, 8, 10)),
    (1597069446000,                datetime.date(2020, 8, 10)),
    ("-1597069446000",             datetime.date(1919, 5, 24)),
    (-1597069446000,               datetime.date(1919, 5, 24)),
    ("null",                       None),
    ("0",                          datetime.date(1970, 1, 1)),
    ("-1",                         datetime.date(1969, 12, 31)),
    ("1",                          datetime.date(1970, 1, 1)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]
fixtures_for_extended_string_unix_timestamp_ms_to_date = [
    ("2020-08-12T12:43:14+0000",   datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z00:00",  None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("2020-08-12T12:43:14Z0000",   None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("  2020-08-12T12:43:14+0000", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0000  ", datetime.date(2020, 8, 12)),
    (" 2020-08-12T12:43:14+0000 ", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+02:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14Z0200",   None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12 12:43:14",        datetime.date(2020, 8, 12)),
    ("2020-08-12",                 datetime.date(2020, 8, 12)),
    (None,                         None),
    ("1597069446000",              datetime.date(2020, 8, 10)),
    (1597069446000,                datetime.date(2020, 8, 10)),
    ("-1597069446000",             datetime.date(1919, 5, 24)),
    (-1597069446000,               datetime.date(1919, 5, 24)),
    ("null",                       None),
    ("0",                          datetime.date(1970, 1, 1)),
    ("-1",                         datetime.date(1969, 12, 31)),
    ("1",                          datetime.date(1970, 1, 1)),
    ("nil",                        None),
    ("Hello",                      None),
    ("2k",                         None),
]
# fmt:on

