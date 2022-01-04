from builtins import str
from builtins import object
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import types as sql_types
from pyspark.sql import functions as F, types as T
from pyspark.sql import Row
import datetime as dt

from spooq.transformer import ThresholdCleaner


# fmt: off
@pytest.fixture(scope="module")
def input_df(spark_session):
    input_data = [
        #  ids   floats  integers    strings         timestamps        datetimes
        [0,       12.0,      12,      "12",  "1850-01-01 12:00:00",  "1850-01-01"],
        [1,       65.7,      65,      "65",  "2020-06-01 12:00:00",  "2020-06-01"],
        [2,      300.0,     300,     "300",  "2020-06-01 15:00:00",  "2020-06-15"],
        [4,     5000.0,    5000,    "5000",  "2020-06-01 16:00:00",  "2020-07-01"],
        [5,      -75.0,     -75,     "-75",  "9999-01-01 12:00:00",  "9999-01-01"],
    ]
    schema = sql_types.StructType(
        [
            sql_types.StructField("id",         sql_types.IntegerType(), True),
            sql_types.StructField("floats",     sql_types.FloatType(),   False),
            sql_types.StructField("integers",   sql_types.IntegerType(), False),
            sql_types.StructField("strings",    sql_types.StringType(),  False),
            sql_types.StructField("timestamps", sql_types.StringType(),  False),
            sql_types.StructField("datetimes",  sql_types.StringType(),  False),
        ]
    )
    input_df = spark_session.createDataFrame(input_data, schema=schema)
    input_df = input_df.withColumn("timestamps", F.col("timestamps").cast(T.TimestampType()))  # Workaround for Timezone
    input_df = input_df.withColumn("datetimes", F.col("datetimes").cast(T.DateType()))  # Workaround for Timezone
    return input_df


@pytest.fixture(scope="class")
def input_df_integers(spark_session):
    input_schema = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("integers", T.IntegerType(), True),
    ])
    return spark_session.createDataFrame(
        [
            (0, -5),
            (1, 5),
            (2, 15),
        ],
        schema=input_schema
    )

@pytest.fixture(scope="class")
def input_df_column_thresholds(spark_session):
    input_schema = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("integer1", T.IntegerType(), True),
        T.StructField("integer2", T.IntegerType(), True),
        T.StructField("min_value", T.IntegerType(), True),
        T.StructField("max_value", T.IntegerType(), True),
    ])
    return spark_session.createDataFrame(
        [
            (0, -5, 0,  0, 10),
            (1,  5, 10, 5, 20),
            (2, 15, 5,  3, 10),
        ],
        schema=input_schema
    )


@pytest.fixture(scope="module")
def thresholds():
    return {
        "integers":   {"min":  1,                    "max":  300},
        "floats":     {"min":  1.0,                  "max":  300.0},
        "strings":    {"min": "1",                   "max": "300"},
        "timestamps": {"min": "2020-06-01 12:00:00", "max": "2020-06-01 16:00:00"},
        "datetimes":  {"min": "2020-06-01",          "max": "2020-07-01"},
    }

@pytest.fixture(scope="module")
def column_thresholds():
    return {
        "integer1":   {"min": F.col("min_value"), "max": F.col("max_value"), "default": -999},
        "integer2":   {"min": 3,                  "max": F.col("max_value"), "default": -999},
    }

@pytest.fixture(scope="module")
def supported_thresholds():
    return {
        "floats":     {"min":  1.0,                  "max":  300.0},
        "integers":   {"min":  1,                    "max":  300},
        "timestamps": {"min": "2020-06-01 08:00:00", "max": "2020-06-01 20:00:00"},
        "datetimes":  {"min": "2020-06-01",          "max": "2020-07-01"},
    }


@pytest.fixture(scope="module")
def expected_result():
    return {
        "integers":   [ 12,    65,    300,   None, None],
        "floats":     [ 12.0,  65.7,  300.0, None, None],
        "strings":    ["12",  "65",  "300",  None, None],
        "timestamps": [None, "2020-06-01 12:00:00", "2020-06-01 15:00:00",
                       "2020-06-01 16:00:00", None],
        "datetimes":  [None, "2020-06-01", "2020-06-15", "2020-07-01", None],
    }
# fmt: on


class TestBasicAttributes(object):
    """Basic attributes of the transformer instance"""

    def test_has_logger(self):
        assert hasattr(ThresholdCleaner(), "logger")

    def test_has_name(self):
        assert ThresholdCleaner().name == "ThresholdCleaner"

    def test_has_str_representation(self):
        assert str(ThresholdCleaner()) == "Transformer Object of Class ThresholdCleaner"


class TestCleaning(object):
    @pytest.mark.parametrize("column_name", ["integers", "floats", "timestamps", "datetimes"])
    def test_clean_supported_format(self, column_name, input_df, thresholds, expected_result):
        thresholds_to_test = {column_name: thresholds[column_name]}
        transformer = ThresholdCleaner(thresholds=thresholds_to_test)
        df_cleaned = transformer.transform(input_df)
        result = [x[column_name] for x in df_cleaned.select(F.col(column_name).cast(T.StringType())).collect()]
        expected = expected_result[column_name]

        pytest.approx(result, 0.01) == expected
        assert input_df.columns == df_cleaned.columns

    def test_multiple_cleansing_rules(self, spark_session, supported_thresholds, input_df):
        transformer = ThresholdCleaner(thresholds=supported_thresholds)
        df_cleaned = transformer.transform(input_df)

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("floats", T.FloatType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField("strings", T.StringType(), True),
                T.StructField("timestamps", T.StringType(), True),
                T.StructField("datetimes", T.StringType(), True),
            ]
        )

        expected_result_df = spark_session.createDataFrame(
            [
                (0, 12.0, 12, "12", None, None),
                (1, 65.7, 65, "65", "2020-06-01 12:00:00", "2020-06-01"),
                (2, 300.0, 300, "300", "2020-06-01 15:00:00", "2020-06-15"),
                (4, None, None, "5000", "2020-06-01 16:00:00", "2020-07-01"),
                (5, None, None, "-75", None, None),
            ],
            schema=expected_output_schema,
        )

        expected_result_df = expected_result_df.withColumn(
            "timestamps", F.col("timestamps").cast(T.TimestampType())
        )  # Workaround for Timezone
        expected_result_df = expected_result_df.withColumn(
            "datetimes", F.col("datetimes").cast(T.DateType())
        )  # Workaround for Timezone

        assert_df_equality(df_cleaned, expected_result_df, ignore_nullable=True)
        assert input_df.columns == df_cleaned.columns

    @pytest.mark.parametrize("column_name", ["strings"])
    def test_raise_exception_for_unsupported_format(self, column_name, input_df, thresholds):
        thresholds_to_test = dict([k_v1 for k_v1 in list(thresholds.items()) if k_v1[0] == column_name])
        transformer = ThresholdCleaner(thresholds_to_test)

        with pytest.raises(ValueError):
            transformer.transform(input_df).count()

    def test_dynamic_default_value(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, num=1),
                Row(id=2, num=2),
                Row(id=3, num=100),
                Row(id=4, num=4),
                Row(id=5, num=-1024),
            ]
        )
        thresholds_to_test = dict(num=dict(min=0, max=99, default=F.col("id") * -1))
        output_df = ThresholdCleaner(thresholds_to_test).transform(input_df)
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, num=1),
                Row(id=2, num=2),
                Row(id=3, num=-3),
                Row(id=4, num=4),
                Row(id=5, num=-5),
            ]
        )
        assert_df_equality(expected_output_df, output_df)


class TestCleansedValuesAreLogged:
    @pytest.fixture(scope="class")
    def transformer(self, supported_thresholds):
        return ThresholdCleaner(supported_thresholds, column_to_log_cleansed_values="cleansed_values_threshold")

    def test_cleansing_log_when_input_value_is_null(self, transformer, spark_session):
        thresholds = dict(integers=dict(min=0, max=10))

        input_schema = T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("integers", T.IntegerType(), True),
        ])
        input_df_integers = spark_session.createDataFrame(
            [
                (0, -5),
                (1, None),
                (2, 15),
            ],
            schema=input_schema
        )

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField(
                    "cleansed_values_threshold",
                    T.StructType(
                        [
                            T.StructField("integers", T.StringType(), True),
                        ]
                    ),
                ),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=0, integers=None, cleansed_values_threshold=Row(integers="-5")),
                Row(id=1, integers=None, cleansed_values_threshold=Row(integers=None)),
                Row(id=2, integers=None, cleansed_values_threshold=Row(integers="15")),
            ],
            schema=expected_output_schema,
        )
        output_df = ThresholdCleaner(thresholds, column_to_log_cleansed_values="cleansed_values_threshold").transform(
            input_df_integers
        )
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_single_cleansed_value_is_stored_in_separate_column(self, transformer, input_df_integers, spark_session):
        thresholds = dict(integers=dict(min=0, max=10))
        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField(
                    "cleansed_values_threshold",
                    T.StructType(
                        [
                            T.StructField("integers", T.StringType(), True),
                        ]
                    ),
                ),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=0, integers=None, cleansed_values_threshold=Row(integers="-5")),
                Row(id=1, integers=5, cleansed_values_threshold=Row(integers=None)),
                Row(id=2, integers=None, cleansed_values_threshold=Row(integers="15")),
            ],
            schema=expected_output_schema,
        )
        output_df = ThresholdCleaner(thresholds, column_to_log_cleansed_values="cleansed_values_threshold").transform(
            input_df_integers
        )
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_single_cleansed_value_is_stored_in_separate_column_with_default_substitute(
        self, transformer, input_df_integers, spark_session
    ):
        thresholds = dict(integers=dict(min=0, max=10, default=-1))
        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField(
                    "cleansed_values_threshold",
                    T.StructType(
                        [
                            T.StructField("integers", T.StringType(), True),
                        ]
                    ),
                ),
            ]
        )

        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=0, integers=-1, cleansed_values_threshold=Row(integers="-5")),
                Row(id=1, integers=5, cleansed_values_threshold=Row(integers=None)),
                Row(id=2, integers=-1, cleansed_values_threshold=Row(integers="15")),
            ],
            schema=expected_output_schema,
        )
        output_df = ThresholdCleaner(thresholds, column_to_log_cleansed_values="cleansed_values_threshold").transform(
            input_df_integers
        )
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules(self, spark_session, transformer, input_df):
        df_cleaned = transformer.transform(input_df)

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("floats", T.FloatType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField("strings", T.StringType(), True),
                T.StructField("timestamps", T.StringType(), True),
                T.StructField("datetimes", T.StringType(), True),
                T.StructField(
                    "cleansed_values_threshold",
                    T.StructType(
                        [
                            T.StructField("floats", T.StringType(), True),
                            T.StructField("integers", T.StringType(), True),
                            T.StructField("timestamps", T.StringType(), True),
                            T.StructField("datetimes", T.StringType(), True),
                        ]
                    ),
                ),
            ]
        )

        expected_result_df = spark_session.createDataFrame(
            [
                (0, 12.0, 12, "12", None, None, (None, None, "1850-01-01 12:00:00", "1850-01-01")),
                (1, 65.7, 65, "65", "2020-06-01 12:00:00", "2020-06-01", (None, None, None, None)),
                (2, 300.0, 300, "300", "2020-06-01 15:00:00", "2020-06-15", (None, None, None, None)),
                (4, None, None, "5000", "2020-06-01 16:00:00", "2020-07-01", ("5000.0", "5000", None, None)),
                (5, None, None, "-75", None, None, ("-75.0", "-75", "9999-01-01 12:00:00", "9999-01-01")),
            ],
            schema=expected_output_schema,
        )

        expected_result_df = expected_result_df.withColumn(
            "timestamps", F.col("timestamps").cast(T.TimestampType())
        )  # Workaround for Timezone
        expected_result_df = expected_result_df.withColumn(
            "datetimes", F.col("datetimes").cast(T.DateType())
        )  # Workaround for Timezone

        assert_df_equality(df_cleaned, expected_result_df, ignore_nullable=True)
        assert input_df.columns + [transformer.column_to_log_cleansed_values] == df_cleaned.columns


class TestCleansedValuesAreLoggedAsMap:
    @pytest.fixture(scope="class")
    def transformer(self, supported_thresholds):
        return ThresholdCleaner(
            supported_thresholds, column_to_log_cleansed_values="cleansed_values_threshold", store_as_map=True
        )

    def test_cleansing_log_when_input_value_is_null(self, transformer, spark_session):
        thresholds = dict(integers=dict(min=0, max=10))

        input_schema = T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("integers", T.IntegerType(), True),
        ])
        input_df_integers = spark_session.createDataFrame(
            [
                (0, -5),
                (1, None),
                (2, 15),
            ],
            schema=input_schema
        )

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField("cleansed_values_threshold", T.MapType(T.StringType(), T.StringType()), True),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                (0, None, {"integers": "-5"}),
                (1, None, None),
                (2, None, {"integers": "15"}),
            ],
            schema=expected_output_schema,
        )

        output_df = ThresholdCleaner(
            thresholds, column_to_log_cleansed_values="cleansed_values_threshold", store_as_map=True
        ).transform(input_df_integers)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_single_cleansed_value_is_stored_in_separate_column(self, transformer, input_df_integers, spark_session):
        thresholds = dict(integers=dict(min=0, max=10))

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField("cleansed_values_threshold", T.MapType(T.StringType(), T.StringType()), True),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                (0, None, {"integers": "-5"}),
                (1, 5, None),
                (2, None, {"integers": "15"}),
            ],
            schema=expected_output_schema,
        )

        output_df = ThresholdCleaner(
            thresholds, column_to_log_cleansed_values="cleansed_values_threshold", store_as_map=True
        ).transform(input_df_integers)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_single_cleansed_value_is_stored_in_separate_column_with_default_substitute(
        self, transformer, input_df_integers, spark_session
    ):
        thresholds = dict(integers=dict(min=0, max=10, default=-1))

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField("cleansed_values_threshold", T.MapType(T.StringType(), T.StringType()), True),
            ]
        )

        expected_output_df = spark_session.createDataFrame(
            [
                (0, -1, {"integers": "-5"}),
                (1, 5, None),
                (2, -1, {"integers": "15"}),
            ],
            schema=expected_output_schema,
        )
        output_df = ThresholdCleaner(
            thresholds, column_to_log_cleansed_values="cleansed_values_threshold", store_as_map=True
        ).transform(input_df_integers)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules(self, spark_session, transformer, input_df):
        df_cleaned = transformer.transform(input_df)

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("floats", T.FloatType(), True),
                T.StructField("integers", T.IntegerType(), True),
                T.StructField("strings", T.StringType(), True),
                T.StructField("timestamps", T.StringType(), True),
                T.StructField("datetimes", T.StringType(), True),
                T.StructField("cleansed_values_threshold", T.MapType(T.StringType(), T.StringType(), True), True),
            ]
        )

        expected_result_df = spark_session.createDataFrame(
            [
                (0, 12.0, 12, "12", None, None, {"timestamps": "1850-01-01 12:00:00", "datetimes": "1850-01-01"}),
                (1, 65.7, 65, "65", "2020-06-01 12:00:00", "2020-06-01", None),
                (2, 300.0, 300, "300", "2020-06-01 15:00:00", "2020-06-15", None),
                (4, None, None, "5000", "2020-06-01 16:00:00", "2020-07-01", {"floats": "5000.0", "integers": "5000"}),
                (
                    5,
                    None,
                    None,
                    "-75",
                    None,
                    None,
                    {
                        "floats": "-75.0",
                        "integers": "-75",
                        "timestamps": "9999-01-01 12:00:00",
                        "datetimes": "9999-01-01",
                    },
                ),
            ],
            schema=expected_output_schema,
        )

        expected_result_df = expected_result_df.withColumn(
            "timestamps", F.col("timestamps").cast(T.TimestampType())
        )  # Workaround for Timezone
        expected_result_df = expected_result_df.withColumn(
            "datetimes", F.col("datetimes").cast(T.DateType())
        )  # Workaround for Timezone

        assert_df_equality(df_cleaned, expected_result_df, ignore_nullable=True)
        assert input_df.columns + [transformer.column_to_log_cleansed_values] == df_cleaned.columns

    def test_column_as_threshold(
            self, transformer, input_df_column_thresholds, spark_session, column_thresholds
    ):

        expected_output_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("integer1", T.IntegerType(), True),
                T.StructField("integer2", T.IntegerType(), True),
                T.StructField("min_value", T.IntegerType(), True),
                T.StructField("max_value", T.IntegerType(), True),
                T.StructField("cleansed_values_threshold", T.MapType(T.StringType(), T.StringType()), True),
            ]
        )

        expected_output_df = spark_session.createDataFrame(
            [
                (0, -999, -999, 0, 10, {"integer1": "-5", "integer2": "0"}),
                (1, 5, 10, 5, 20, None),
                (2, -999, 5, 3, 10, {"integer1": "15"}),
            ],
            schema=expected_output_schema,
        )
        output_df = ThresholdCleaner(
            column_thresholds, column_to_log_cleansed_values="cleansed_values_threshold", store_as_map=True,
        ).transform(input_df_column_thresholds)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)