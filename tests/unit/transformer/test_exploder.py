from builtins import str
from builtins import object
import pytest
import json
from pyspark.sql import functions as sql_funcs
from pyspark.sql import Row

from spooq2.transformer import Exploder


class TestBasicAttributes(object):
    """Mapper for Exploding Arrays"""

    def test_logger_should_be_accessible(self):
        assert hasattr(Exploder(), "logger")

    def test_name_is_set(self):
        assert Exploder().name == "Exploder"

    def test_str_representation_is_correct(self):
        assert str(Exploder()) == "Transformer Object of Class Exploder"


class TestExploding(object):
    @pytest.fixture(scope="module")
    def input_df(self, spark_session):
        return spark_session.read.parquet("data/schema_v1/parquetFiles")

    @pytest.fixture()
    def default_params(self):
        return {"path_to_array": "attributes.friends", "exploded_elem_name": "friend"}

    @pytest.mark.slow
    def test_count(self, input_df, default_params):
        expected_count = input_df.select(sql_funcs.explode_outer(input_df[default_params["path_to_array"]])).count()
        actual_count = Exploder(**default_params).transform(input_df).count()
        assert expected_count == actual_count

    @pytest.mark.slow
    def test_exploded_array_is_added(self, input_df, default_params):
        transformer = Exploder(**default_params)
        expected_columns = set(input_df.columns + [default_params["exploded_elem_name"]])
        actual_columns = set(transformer.transform(input_df).columns)

        assert expected_columns == actual_columns

    @pytest.mark.slow
    def test_array_is_converted_to_struct(self, input_df, default_params):
        def get_data_type_of_column(df, path=["attributes"]):
            record = df.first().asDict(recursive=True)
            for p in path:
                record = record[p]
            return type(record)

        current_data_type_friend = get_data_type_of_column(input_df, path=["attributes", "friends"])
        assert issubclass(current_data_type_friend, list)

        transformed_df = Exploder(**default_params).transform(input_df)
        transformed_data_type = get_data_type_of_column(transformed_df, path=["friend"])

        assert issubclass(transformed_data_type, dict)

    def test_records_with_empty_arrays_are_dropped_by_default(self, spark_session):
        input_df = spark_session.createDataFrame([
            Row(id=1, array_to_explode=[]),
            Row(id=2, array_to_explode=[Row(elem_id="a"), Row(elem_id="b"), Row(elem_id="c")]),
            Row(id=3, array_to_explode=[]),
        ])
        transformed_df = Exploder(path_to_array="array_to_explode", exploded_elem_name="elem").transform(input_df)
        assert transformed_df.count() == 5

    def test_records_with_empty_arrays_are_kept_via_setting(self, spark_session):
        input_df = spark_session.createDataFrame([
            Row(id=1, array_to_explode=[]),
            Row(id=2, array_to_explode=[Row(elem_id="a"), Row(elem_id="b"), Row(elem_id="c")]),
            Row(id=3, array_to_explode=[]),
        ])
        transformed_df = Exploder(path_to_array="array_to_explode",
                                  exploded_elem_name="elem",
                                  drop_rows_with_empty_array=True).transform(input_df)
        assert transformed_df.count() == 3

