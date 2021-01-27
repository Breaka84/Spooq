from builtins import str
from builtins import object
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import types as sql_types
from pyspark.sql import functions as F
from pyspark.sql import Row
import datetime as dt

from spooq2.transformer import ThresholdCleaner


class TestBasicAttributes(object):
    """ Basic attributes of the transformer instance """

    def test_has_logger(self):
        assert hasattr(ThresholdCleaner(), "logger")

    def test_has_name(self):
        assert ThresholdCleaner().name == "ThresholdCleaner"

    def test_has_str_representation(self):
        assert str(ThresholdCleaner()) == "Transformer Object of Class ThresholdCleaner"


class TestCleaning(object):

    # fmt: off
    @pytest.fixture(scope="module")
    def input_df(self, spark_session):
        input_data = [
            #ids  #floats   #integers  #strings         #timestamps           #datetimes
            [0,       12.0,      12,      "12",  dt.datetime(1850,1,1, 12,0,0), dt.date(1850,1,1)],
            [1,       65.7,      65,      "65",  dt.datetime(2020,6,1, 12,0,0), dt.date(2020,6,1)],
            [2,      300.0,     300,     "300",  dt.datetime(2020,6,1, 15,0,0), dt.date(2020,6,15)],
            [4,     5000.0,    5000,    "5000",  dt.datetime(2020,6,1, 16,0,0), dt.date(2020,7,1)],
            [5,      -75.0,     -75,     "-75",  dt.datetime(9999,1,1, 12,0,0), dt.date(9999,1,1)],
        ]
        schema = sql_types.StructType(
            [
                sql_types.StructField("id",         sql_types.IntegerType(),   True),
                sql_types.StructField("floats",     sql_types.DoubleType(),    False),
                sql_types.StructField("integers",   sql_types.LongType(),      False),
                sql_types.StructField("strings",    sql_types.StringType(),    False),
                sql_types.StructField("timestamps", sql_types.TimestampType(), False),
                sql_types.StructField("datetimes",  sql_types.DateType(),      False),
            ]
        )
        return spark_session.createDataFrame(input_data, schema=schema)

    @pytest.fixture(scope="module")
    def thresholds(self):
        return {
            "integers":   {"min":  1,                           "max":  300},
            "floats":     {"min":  1.0,                         "max":  300.0},
            "strings":    {"min": "1",                          "max": "300"},
            "timestamps": {"min": dt.datetime(2020,6,1,12,0,0), "max": dt.datetime(2020,6,1,16,0,0)},
            "datetimes":  {"min": dt.date(2020,6,1),            "max": "2020-7-1"},
        }

    @pytest.fixture(scope="module")
    def expected_result(self):
        return {
            "integers":   [ 12,    65,    300,   None, None],
            "floats":     [ 12.0,  65.7,  300.0, None, None],
            "strings":    ["12",  "65",  "300",  None, None],
            "timestamps": [None, dt.datetime(2020,6,1, 12,0,0), dt.datetime(2020,6,1, 15,0,0),
                           dt.datetime(2020,6,1, 16,0,0), None],
            "datetimes":  [None, dt.date(2020,6,1), dt.date(2020,6,15), dt.date(2020,7,1), None],
        }
    # fmt: on

    @pytest.mark.parametrize("column_name", ["integers", "floats", "timestamps", "datetimes"])
    def test_clean_supported_format(self, column_name, input_df, thresholds, expected_result):
        thresholds_to_test = {column_name: thresholds[column_name]}
        transformer = ThresholdCleaner(thresholds=thresholds_to_test)
        df_cleaned = transformer.transform(input_df)
        result = [x[column_name] for x in df_cleaned.collect()]
        expected = expected_result[column_name]

        assert result == expected
        assert input_df.columns == df_cleaned.columns

    @pytest.mark.parametrize("column_name", ["strings"])
    def test_raise_exception_for_unsupported_format(self, column_name, input_df, thresholds):
        thresholds_to_test = dict([k_v1 for k_v1 in list(thresholds.items()) if k_v1[0] == column_name])
        transformer = ThresholdCleaner(thresholds_to_test)

        with pytest.raises(ValueError):
            transformer.transform(input_df).count()

    def test_dynamic_default_value(self, spark_session):
        input_df = spark_session.createDataFrame([
            Row(id=1, num=1),
            Row(id=2, num=2),
            Row(id=3, num=100),
            Row(id=4, num=4),
            Row(id=5, num=-1024),
        ])
        thresholds_to_test = dict(num=dict(min=0, max=99, default=F.col("id") * -1))
        output_df = ThresholdCleaner(thresholds_to_test).transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            Row(id=1, num=1),
            Row(id=2, num=2),
            Row(id=3, num=-3),
            Row(id=4, num=4),
            Row(id=5, num=-5),
        ])
        assert_df_equality(expected_output_df, output_df)
