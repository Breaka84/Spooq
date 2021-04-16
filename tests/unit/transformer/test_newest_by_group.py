from builtins import str
from builtins import object
import pytest
import pandas as pd
from pyspark.sql import types as sql_types

from spooq2.transformer import NewestByGroup


@pytest.fixture(scope="module")
def input_df(spark_session):
    # fmt: off
    input_data = [
        # id  secondary_id  idx  attr_1    attr_2  updated_at  deleted_at
        [1,            2,   0,      1, "attr_2_2",       20,        50],
        [2,            3,   1,      4, "attr_2_3",       50,        50],
        [2,            3,   2,      4, "attr_2_3",       55,        60],
        [2,            1,   3,      4, "attr_2_1",       45,        45],
        [3,            3,   4,      9, "attr_2_3",       45,        45],
        [4,            5,   5,     16, "attr_2_5",       60,        60],
        [6,            7,   6,     36, "attr_2_7",       55,        55],
        [2,            7,   7,      4, "attr_2_7",       55,        50],
        [7,            7,   8,     49, "attr_2_7",       35,        35],
        [1,            1,   9,      1, "attr_2_1",       55,        55],
        [4,            2,  10,     16, "attr_2_2",       50,        70],
        [6,            7,  11,     36, "attr_2_7",       60,      None],
        [7,            7,  12,     49, "attr_2_7",     None,       100],
        [5,            4,  13,     25, "attr_2_4",       40,        40],
        [5,            4,  14,     25, "attr_2_4",       40,        45],
    ]

    schema = sql_types.StructType([
        sql_types.StructField("id"          , sql_types.IntegerType(), False),
        sql_types.StructField("secondary_id", sql_types.IntegerType(), False),
        sql_types.StructField("idx"         , sql_types.IntegerType(), False),
        sql_types.StructField("attr_1"      , sql_types.IntegerType(), True),
        sql_types.StructField("attr_2"      , sql_types.StringType(),  True),
        sql_types.StructField("updated_at"  , sql_types.IntegerType(), True),
        sql_types.StructField("deleted_at"  , sql_types.IntegerType(), True),
    ])
    # fmt: on

    return spark_session.createDataFrame(input_data, schema=schema)


class TestBasicAttributes(object):
    """Transformer to Group, Sort and Select the Top Row per Group"""

    def test_logger_should_be_accessible(self):
        assert hasattr(NewestByGroup(), "logger")

    def test_name_is_set(self):
        assert NewestByGroup().name == "NewestByGroup"

    def test_str_representation_is_correct(self):
        assert str(NewestByGroup()) == "Transformer Object of Class NewestByGroup"


class TestTransformMethod(object):
    """Transform Method"""

    # fmt: off
    id_to_index_params = [
        (1,  9),  # highest updated_at
        (2,  2),  # same updated_at, highest deleted_at
        (3,  4),  # only row
        (4,  5),  # highest updated_at, though deleted_at would be higher of index 10
        (6, 11),  # highest updated_at, though deleted_at is NULL
        (7,  8)   # updated_at is NULL, though deleted_at would be higher of index 12
    ]
    # fmt: on
    @pytest.mark.parametrize(("id", "index"), id_to_index_params)
    def test_transform_method(self, input_df, id, index):
        """Correct Row per Group (Single Column) is returned"""
        transformed_df = NewestByGroup(group_by="id", order_by="updated_at").transform(input_df)
        assert transformed_df.where("id = {id}".format(id=id)).collect()[0].idx == index

    # fmt: off
    two_ids_to_index_params = [
        ((1,  1),  9),  # Only occurence
        ((1,  2),  0),  # Only occurence
        ((2,  1),  3),  # Only occurence
        ((2,  3),  2),  # Higher updated_at
        ((2,  7),  7),  # Only occurence
        ((3,  3),  4),  # Only occurence
        ((4,  2), 10),  # Only occurence
        ((4,  5),  5),  # Only occurence
        ((5,  4), 14),  # same updated_at, highest deleted_at
        ((6,  7), 11),  # highest updated_at, though deleted_at is NULL
        ((7,  7),  8)   # updated_at is NULL, though deleted_at would be higher of index 12
    ]
    # fmt: on
    @pytest.mark.parametrize(("ids", "index"), two_ids_to_index_params)
    def test_transform_method_multipe_grouping_ids(self, input_df, ids, index):
        """Correct Row per Group (Multiple Columns) is returned"""
        transformer = NewestByGroup(group_by=["id", "secondary_id"])
        transformed_df = transformer.transform(input_df)
        assert (
            transformed_df.where("id = {id1} and secondary_id = {id2}".format(id1=ids[0], id2=ids[1])).collect()[0].idx
            == index
        )

    def test_transform_method_input_as_string(self, input_df):
        """Columns to Group by and Sort by are passed as Strings"""
        id = 4
        idx = 5
        transformer = NewestByGroup(group_by="id", order_by="updated_at")
        transformed_df = transformer.transform(input_df)
        assert transformed_df.where("id = {id}".format(id=id)).collect()[0].idx == idx
