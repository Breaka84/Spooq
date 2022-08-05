import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row, DataFrame


# fmt:off

def get_ids_for_fixture(fixtures):
    id_strings = []
    for test_fixture in fixtures:
        id_string = []
        if not isinstance(test_fixture, (list, set, tuple)):
            test_fixture = [test_fixture]
        for idx, param in enumerate(test_fixture, start=1):
            try:
                param_value = ", ".join(param.toJSON().collect())
            except AttributeError:
                param_value = str(param)
            id_string.append(f"Param {idx}: <{param_value}> (type: {type(param)})")
        id_strings.append(" | ".join(id_string))

    return id_strings


complex_event_expression = (
    F.when(F.col("nested.input_key_1").isNotNull(), F.col("nested.input_key_1") / 1000)
    .otherwise(F.col("nested.input_key_2") / 1000)
    .cast(T.TimestampType())
    .cast(T.DateType())
)

fixtures_for_spark_sql_object = [
    # input_value_1           # input_value_2        # mapper function                   # expected_value
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

fixtures_for_as_is = [
    ("only some text",
     "only some text"),
    (None,
     None),
    ({"key": "value"},
     {"key": "value"}),
    ({"key": {"other_key": "value"}},
     {"key": {"other_key": "value"}}),
    ({"age": 18, "weight": 75},
     {"age": 18, "weight": 75}),
    ({"list_of_friend_ids": [12, 75, 44, 76]},
     {"list_of_friend_ids": [12, 75, 44, 76]}),
    ([{"weight": "75"}, {"weight": "76"}, {"weight": "73"}],
     [{"weight": "75"}, {"weight": "76"}, {"weight": "73"}]),
    ({"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]},
     {"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]}),
]

fixtures_for_json_string = [
    ("only some text",
     "only some text"),
    (None,
     None),
    ({"key": "value"},
     '{"key": "value"}'),
    ({"key": {"other_key": "value"}},
     '{"key": {"other_key": "value"}}'),
    ({"age": 18, "weight": 75},
     '{"age": 18, "weight": 75}'),
    ({"list_of_friend_ids": [12, 75, 44, 76]},
     '{"list_of_friend_ids": [12, 75, 44, 76]}'),
    ([{"weight": "75"}, {"weight": "76"}, {"weight": "73"}],
     '[{"weight": "75"}, {"weight": "76"}, {"weight": "73"}]'),
    ({"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]},
     '{"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]}')
]

fixtures_for_timestamp_to_first_of_month = [
    (None,         None),
    ("1955-09-41", None),
    ("1969-04-03", "1969-04-01"),
    ("1985-03-07", "1985-03-01"),
    ("1998-06-10", "1998-06-01"),
    ("1967-05-16", "1967-05-01"),
    ("1953-01-01", "1953-01-01"),
    ("1954-11-06", "1954-11-01"),
    ("1978-09-05", "1978-09-01"),
    ("1999-05-23", "1999-05-01"),
]

fixtures_for_meters_to_cm = [
    (1.80,    180),
    (2.,      200),
    (-1.0,   -100),
    (0.0,       0),
    (0,         0),
    (2,       200),
    (-4,     -400),
    ("1.80",  180),
    ("2.",    200),
    ("-1.0", -100),
    ("0.0",     0),
    (None,   None),
    ("one",  None),
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

fixtures_for_str_to_int = [
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

fixtures_for_str_to_long = [
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

fixtures_for_str_to_float = [
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

fixtures_for_str_to_double = [
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

fixtures_for_to_bool_base = [
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
    ("ja",       None),
    ("Nein",     None),
    ("   true",  True),
    ("true   ",  True),
    (" true  ",  True),
    (1,          True),
    (0,          False),
    (-1,         None),
    (1.0,        None),
    (0.0,        None),
    (-1.0,       None),
    (123,        None),
    (-123,       None),
    (True,       True),
    (False,      False),
]

fixtures_for_to_bool_default = [
    ("enabled",  True),
    ("ENABLED",  True),
    ("disabled", False),
    ("Disabled", False),
    ("on",       True),
    ("ON",       True),
    ("off",      False),
    ("Off",      False),
] + fixtures_for_to_bool_base

fixtures_for_to_bool_true_values_as_argument = [
    ("Sure",    True),
    ("sure",    True),
    ("surely",  None),
    ("OK",      True),
    ("ok",      True),
]

fixtures_for_to_bool_false_values_as_argument = [
    ("Nope",    False),
    ("nope",    False),
    ("Nope!",   None),
    ("NOK",     False),
    ("Nok",     False),
]

fixtures_for_to_bool_true_and_false_values_as_argument = set(
    fixtures_for_to_bool_false_values_as_argument + fixtures_for_to_bool_false_values_as_argument
)

fixtures_for_to_bool_true_values_added = set(
    fixtures_for_to_bool_base + fixtures_for_to_bool_true_values_as_argument
)

fixtures_for_to_bool_false_values_added = set(
    fixtures_for_to_bool_base + fixtures_for_to_bool_false_values_as_argument
)

fixtures_for_to_bool_true_and_false_values_added = set(
    fixtures_for_to_bool_true_values_added.union(fixtures_for_to_bool_false_values_added)
)

fixtures_for_extended_string_to_timestamp_spark2 = [
    ("2020-08-12T12:43:14+0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z00:00",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14Z0000",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("  2020-08-12T12:43:14+0000", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+0000  ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    (" 2020-08-12T12:43:14+0000 ", datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+02:00",  datetime.datetime(2020, 8, 12, 10, 43, 14)),
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
    ("1597069446002",              datetime.datetime(2020, 8, 10, 14, 24, 6, 2000)),
    (1597069446000,                datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446002,                datetime.datetime(2020, 8, 10, 14, 24, 6, 2000)),
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

fixtures_for_to_timestamp_default = [
    ("2020-08-12T12:43:14+0000",    datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+00:00",   datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14.543000",  datetime.datetime(2020, 8, 12, 12, 43, 14, 543000)),
    ("2020-08-12T12:43:14Z00:00",   None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("2020-08-12T12:43:14Z0000",    None),  # No `Z` allowed by Spark3 for timezone settings (only `+HH:MM`)
    ("  2020-08-12T12:43:14+0000",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+0000  ",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    (" 2020-08-12T12:43:14+0000 ",  datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12T12:43:14+02:00",   datetime.datetime(2020, 8, 12, 10, 43, 14)),
    ("2020-08-12T12:43:14+0200",    {"<=3.2.0": None, ">=3.2.1": datetime.datetime(2020, 8, 12, 10, 43, 14)}),
    ("2020-08-12T12:43:14Z0200",    None),  # only `+HH:MM` is supported by Spark for timezone offsets
    ("2020-08-12T12:43:14",         datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12 12:43:14",         datetime.datetime(2020, 8, 12, 12, 43, 14)),
    ("2020-08-12",                  datetime.datetime(2020, 8, 12, 0, 0, 0)),
    (None,                          None),
    ("1597069446",                  datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446,                    datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("-1597069446",                 datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446,                   datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("1597069446000",               datetime.datetime(2020, 8, 10, 14, 24, 6)),
    ("1597069446002",               datetime.datetime(2020, 8, 10, 14, 24, 6, 2000)),
    (1597069446000,                 datetime.datetime(2020, 8, 10, 14, 24, 6)),
    (1597069446002,                 datetime.datetime(2020, 8, 10, 14, 24, 6, 2000)),
    ("-1597069446000",              datetime.datetime(1919, 5, 24, 9, 35, 54)),
    (-1597069446000,                datetime.datetime(1919, 5, 24, 9, 35, 54)),
    ("null",                        None),
    ("0",                           datetime.datetime(1970, 1, 1, 0, 0, 0)),
    ("-1",                          datetime.datetime(1969, 12, 31, 23, 59, 59)),
    ("1",                           datetime.datetime(1970, 1, 1, 0, 0, 1)),
    ("nil",                         None),
    ("Hello",                       None),
    ("2k",                          None),
    ("-5355957305054330880",        None),
    ("5355957305054330880",         None),
    (-5355957305054330880,          None),
    ("1.547035982469E12",           None),
]

fixtures_for_to_timestamp_custom_input_format = [
    # input_value,           # input_format             # expected_output
    ("20",                   "yy",                      "2020-01-01 00:00:00"),
    ("20",                   "yyyy",                    None),
    (20,                     "yy",                      "2020-01-01 00:00:00"),
    ("2020",                 "yyyy",                    "2020-01-01 00:00:00"),
    (2020,                   "yyyy",                    "2020-01-01 00:00:00"),
    ("202012",               "yyyyMM",                  "2020-12-01 00:00:00"),
    (202012,                 "yyyyMM",                  "2020-12-01 00:00:00"),
    ("20201224",             "yyyyMMdd",                "2020-12-24 00:00:00"),
    (20201224,               "yyyyMMdd",                "2020-12-24 00:00:00"),
    (20201324,               "yyyyMMdd",                None),
    ("2020/12/24",           "yyyy/MM/dd",              "2020-12-24 00:00:00"),
    ("2020/12/24-20",        "yyyy/MM/dd-HH",           "2020-12-24 20:00:00"),
    ("2020/12/24 20:07",     "yyyy/MM/dd HH:mm",        "2020-12-24 20:07:00"),
    ("2020/12/24 20:07:35",  "yyyy/MM/dd HH:mm:ss",     "2020-12-24 20:07:35"),
]

fixtures_for_to_timestamp_custom_output_format = [
    # Input date is "2020-12-24 20:07:35.253"
    # date_format,           # expected string
    ("yy",                   "20"),
    ("yyyy",                 "2020"),
    ("yyyyMM",               "202012"),
    ("yyyyMMdd",             "20201224"),
    ("yyyy/MM/dd",           "2020/12/24"),
    ("yyyy/MM/dd-HH",        "2020/12/24-20"),
    ("yyyy/MM/dd HH:mm",     "2020/12/24 20:07"),
    ("yyyy/MM/dd HH:mm:ss",  "2020/12/24 20:07:35"),
]

fixtures_for_to_timestamp_max_valid_timestamp_in_sec = [
    # Input date is 1608840455 "2020-12-24 20:07:35"
    # max_valid_timestamp,  # expected timestamp
    (4102358400,            "2020-12-24 20:07:35"), #datetime.datetime(2020, 12, 24, 20,  7, 35)),       # max valid: 2099-12-31 00:00:00 UTC, default
    (1608843600,            "2020-12-24 20:07:35"), #datetime.datetime(2020, 12, 24, 20,  7, 35)),       # max valid: 2020-12-24 21:00:00 UTC
    (1608685200,            "1970-01-19 14:54:00.455"), #datetime.datetime(1970, 1,  19,  2, 53, 56, 400)),  # max valid: 2020-12-23 01:00:00 UTC
]

fixtures_for_to_timestamp_min_max_limits = [
    # input_value,      # expected timestamp
    (-62135514321000,   "0001-01-01 22:54"),  # Minimum supported
    (-62135514321001,   None),  # Minimum of datetime Python lib (-1ms)
    (253402210800000,   "9999-12-30 23:00"),  # Maximum supported
    (253402210800001,   None),  # Maximum of datetime Python lib (+1ms)
]

fixtures_for_to_str = [
    (12345,       "12345"),
    (-12345,      "-12345"),
    (12345.0,     "12345.0"),
    (-12345.0,    "-12345.0"),
    ("Hello",     "Hello"),
    ("   Hello",  "   Hello"),
    ("Hello   ",  "Hello   "),
    (True,        "true"),
    (False,       "false"),
    (None,        None),
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
    ("2020-08-12T12:43:14Z00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z0000",   datetime.date(2020, 8, 12)),
    ("  2020-08-12T12:43:14+0000", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0000  ", datetime.date(2020, 8, 12)),
    (" 2020-08-12T12:43:14+0000 ", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+02:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0200",   datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z0200",   datetime.date(2020, 8, 12)),
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
    ("2020-08-12T12:43:14Z00:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z0000",   datetime.date(2020, 8, 12)),
    ("  2020-08-12T12:43:14+0000", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+0000  ", datetime.date(2020, 8, 12)),
    (" 2020-08-12T12:43:14+0000 ", datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14+02:00",  datetime.date(2020, 8, 12)),
    ("2020-08-12T12:43:14Z0200",   datetime.date(2020, 8, 12)),
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

fixtures_for_timestamp_ms_to_s = [
    (              1,            0),
    (              0,            0),
    (             -1,            0),
    (           None,         None),
    (  1637334764123,   1637334764),
    (  4102358400000,   4102358400),
    (  4102358400001,   4102358400),
    (  5049688276000,   5049688276),
    (  3469296996000,   3469296996),
    (  7405162940000,   7405162940),
    (  2769601503000,   2769601503),
    ( -1429593275000,  -1429593275),
    (  3412549669000,   3412549669),
    ("2769601503000",   2769601503),
]

fixtures_for_timestamp_s_to_ms = [
    (               1,            1000),
    (               0,               0),
    (              -1,           -1000),
    (            None,            None),
    (  1637334764.123,   1637334764123),
    (      4102358400,   4102358400000),
    (      5049688276,   5049688276000),
    (      3469296996,   3469296996000),
    (      7405162940,   7405162940000),
    (      2769601503,   2769601503000),
    (     -1429593275,  -1429593275000),
    (      3412549669,   3412549669000),
    (    "2769601503",   2769601503000),
]

fixtures_for_str_to_array_str_to_int = [
    # input_string          # expected_int_array
    (" [1,2,3]",            [1, 2, 3]),
    (" []",                 [None]),
    (" ]",                  [None]),
    ("1,2,test",            [1, 2, None]),
    ("t,est",               [None, None]),
    ("1,  2   ,  test",     [1, 2, None]),
    ("   1,  2.4   ,  4  ", [1, 2, 4]),
    (None,                  None),
]

fixtures_for_str_to_array_str_to_str = [
    # input_string                       # expected_str_array
    ("[item1,item2,3]",                  ["item1", "item2", "3"]),
    ("item1,it[e]m2,it em3",             ["item1", "it[e]m2", "it em3"]),
    ("    item1,   item2    ,   item3",  ["item1", "item2", "item3"]),
    ("[it em1, item2, item3",            ["it em1", "item2", "item3"]),
    ("[it ] e [ m1 , [ item2 ] , item3", ["it ] e [ m1", "[ item2 ]", "item3"]),
    (" [ item1 , item2, item3 ] ",       ["item1", "item2", "item3"]),
    (None,                               None),
]

fixtures_for_map_values_string_for_string_without_default_case_sensitive = [
    # mapping = {"whitelist": "allowlist", "blacklist": "blocklist"}
    # ignore_case = False
    # input_value,   # expected_output
    ("allowlist",    "allowlist"),
    ("WhiteList",    "WhiteList"),  # case sensitive
    ("blocklist",    "blocklist"),
    ("blacklist",    "blocklist"),
    ("Blacklist",    "Blacklist"),  # case sensitive
    ("Shoppinglist", "Shoppinglist"),
    (None,           None),
    (1,              "1"),
    (True,           "true"),
    (False,          "false"),
]

fixtures_for_map_values_string_for_string_without_default = [
    # mapping = {"whitelist": "allowlist", "blacklist": "blocklist"}
    # input_value,   # expected_output
    ("allowlist",    "allowlist"),
    ("WhiteList",    "allowlist"),
    ("blocklist",    "blocklist"),
    ("blacklist",    "blocklist"),
    ("Blacklist",    "blocklist"),
    ("Shoppinglist", "Shoppinglist"),
    (None,           None),
    (1,              "1"),
    (True,           "true"),
    (False,          "false"),
]

fixtures_for_map_values_sql_like_pattern = [
    # mapping = {"%white%": True, "%black%": True}
    # pattern_type = "sql_like"
    # default = False
    # output_type = T.BooleanType()
    # input_value,   # expected_output
    ("allowlist",    False),
    ("WhiteList",    True),
    ("blocklist",    False),
    ("blacklist",    True),
    ("Blacklist",    True),
    ("Shoppinglist", False),
    (None,           False),
    (1,              False),
    (True,           False),
    (False,          False),
]

fixtures_for_map_values_regex_pattern = [
    # mapping = {"white": True, ".*black.*": True}
    # pattern_type = "regex"
    # default = False
    # output_type = T.BooleanType()
    # input_value,   # expected_output
    ("allowlist",    False),
    ("WhiteList",    True),
    ("blocklist",    False),
    ("blacklist",    True),
    ("Blacklist",    True),
    ("Shoppinglist", False),
    (None,           False),
    (1,              False),
    (True,           False),
    (False,          False),
]

fixtures_for_map_values_string_for_string_with_default = [
    # mapping = {"whitelist": "allowlist", "blacklist": "blocklist"}
    # default = "No mapping found!"
    # input_value,   # expected_output
    ("allowlist",    "No mapping found!"),
    ("whitelist",    "allowlist"),
    ("WhiteList",    "allowlist"),
    ("blocklist",    "No mapping found!"),
    ("blacklist",    "blocklist"),
    ("Blacklist",    "blocklist"),
    ("Shoppinglist", "No mapping found!"),
    (None,           "No mapping found!"),
    (1,              "No mapping found!"),
    (True,           "No mapping found!"),
    (False,          "No mapping found!"),
]

fixtures_for_map_values_string_for_string_with_dynamic_default = [
    # mapping = {"whitelist": "allowlist", "blacklist": "blocklist"}
    # default = F.length("mapped_name")
    # input_value,   # expected_output
    ("allowlist",    "9"),
    ("whitelist",    "allowlist"),
    ("WhiteList",    "allowlist"),
    ("blocklist",    "9"),
    ("blacklist",    "blocklist"),
    ("Blacklist",    "blocklist"),
    ("Shoppinglist", "12"),
    (None,           None),
    (1,              "1"),
    (True,           "4"),
    (False,          "5"),
]

fixtures_for_map_values_string_for_integer = [
    # mapping = {0: "bad", 1: "ok", 2: "good"}
    # default = source_column
    # input_value,   # expected_output
    (-1,    "-1"),
    ("-1",  "-1"),
    (0,     "bad"),
    ("0",   "bad"),
    (1,     "ok"),
    ("1",   "ok"),
    (2,     "good"),
    ("2",   "good"),
    (3,     "3"),
    ("3",   "3"),
    (None,  None),
    (True,  "true"),
    (False, "false"),
]

fixtures_for_map_values_integer_for_string = [
    # mapping = {"0": -99999}
    # default = source_column
    # output_type = T.LongType()
    # input_value, # expected_output
    (-1,    -1),
    ("-1",  -1),
    (0,     -99999),
    ("0",   -99999),
    (1,     1),
    ("1",   1),
    (None,  None),
    (True,  1),
    (False, 0),
]

fixtures_for_apply_set_to_lower_case = [
    # func = F.lower
    # input_value,   # expected_output
    (-1,            "-1"),
    (99,            "99"),
    ("-1",          "-1"),
    ("99",          "99"),
    ("Hello World", "hello world"),
    ("hello world", "hello world"),
    (None,          None),
]

fixtures_for_apply_check_if_user_still_has_hotmail = [
    # input_value,   # expected_output
    ("sarajishvilileqso@gmx.at", False),
    ("jnnqn@astrinurdin.art",    False),
    ("321aw@hotmail.com",        True),
    ("techbrenda@hotmail.com",   True),
    ("sdsxcx@gmail.com",         False),
]

fixtures_for_apply_check_if_number_is_even = [
    # input_value,   # expected_output
    ("21474836470",  True),
    ("21474836473",  False),
    (21474836470,    True),
    (21474836473,    False),
    (21474836470.10, True),
    (21474836473.13, False),
    (-1,             False),
    (0,              True),
    (1,              False),
    ("NULL",         False),
    ("None",         False),
    (None,           False),
    ("Hello World",  False),
]

# fmt: on
