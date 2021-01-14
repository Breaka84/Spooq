import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
import datetime

from spooq2.transformer import Flattener


@pytest.fixture
def flattener():
    return Flattener()


class TestBasicAttributes:
    """Mapper for Flattening DataFrames"""

    def test_logger_should_be_accessible(self):
        assert hasattr(Flattener(), "logger")

    def test_name_is_set(self):
        assert Flattener().name == "Flattener"

    def test_str_representation_is_correct(self):
        assert str(Flattener()) == "Transformer Object of Class Flattener"


class TestAlreadyFlatDataFrames:
    def test_single_column(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            string_val="Hello World"
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            ("Hello World", )], schema=["string_val"])

        assert_df_equality(expected_output_df, output_df)

    def test_multiple_columns_of_same_datatype(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            int_val_1=4789,
            int_val_2=4790,
            int_val_3=4791
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, 4790, 4791)], schema=["int_val_1", "int_val_2", "int_val_3"])

        assert_df_equality(expected_output_df, output_df)

    def test_multiple_columns_of_different_datatype(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            int_val=4789,
            string_val="Hello World",
            date_val=datetime.date(2021, 1, 14)
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, "Hello World", datetime.date(2021, 1, 14))], schema=["int_val", "string_val", "date_val"])

        assert_df_equality(expected_output_df, output_df)


class TestDataFrameContainingArrays:
    def test_single_array(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val=[4789, 4790, 4791]
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, ), (4790, ), (4791, )], schema=["array_val_exploded"])

        assert_df_equality(expected_output_df, output_df)

    def test_single_array_with_other_columns(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val=[4789, 4790, 4791],
            timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14)
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (datetime.datetime(2021, 1, 14, 8, 10, 14), 4789),
            (datetime.datetime(2021, 1, 14, 8, 10, 14), 4790),
            (datetime.datetime(2021, 1, 14, 8, 10, 14), 4791)],
            schema=["timestamp_val", "array_val_exploded"])

        assert_df_equality(expected_output_df, output_df)

    def test_multiple_arrays(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val_1=[4789, 4790, 4791],
            array_val_2=["How", "Are", "You", "?"]
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, "How"),
            (4789, "Are"),
            (4789, "You"),
            (4789, "?"),
            (4790, "How"),
            (4790, "Are"),
            (4790, "You"),
            (4790, "?"),
            (4791, "How"),
            (4791, "Are"),
            (4791, "You"),
            (4791, "?")],
            schema=["array_val_1_exploded", "array_val_2_exploded"])

        assert_df_equality(expected_output_df, output_df)

    def test_multiple_arrays_with_other_columns(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val_1=[4789, 4790, 4791],
            array_val_2=["How", "Are", "You", "?"],
            double_val=43.102
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (43.102, 4789, "How"),
            (43.102, 4789, "Are"),
            (43.102, 4789, "You"),
            (43.102, 4789, "?"),
            (43.102, 4790, "How"),
            (43.102, 4790, "Are"),
            (43.102, 4790, "You"),
            (43.102, 4790, "?"),
            (43.102, 4791, "How"),
            (43.102, 4791, "Are"),
            (43.102, 4791, "You"),
            (43.102, 4791, "?")],
            schema=["double_val", "array_val_1_exploded", "array_val_2_exploded"])

        assert_df_equality(expected_output_df, output_df)

    def test_array_nested_in_array(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val=[["Here's", "My", "Number", ":"], [555, 127, 53, 90]],
            string_val="How are you?"
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            ("How are you?", "Here's"),
            ("How are you?", "My"),
            ("How are you?", "Number"),
            ("How are you?", ":"),
            ("How are you?", "555"),
            ("How are you?", "127"),
            ("How are you?", "53"),
            ("How are you?", "90")],
            schema=["string_val", "array_val_exploded_exploded"])

        assert_df_equality(expected_output_df, output_df)

    def test_array_nested_in_struct(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            struct_val=Row(array_val=[4789, 4790, 4791],
                           string_val="How are you?")
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            ("How are you?", 4789),
            ("How are you?", 4790),
            ("How are you?", 4791)],
            schema=["struct_val_string_val", "struct_val_array_val_exploded"])

        assert_df_equality(expected_output_df, output_df)

    def test_struct_nested_in_array(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val=[Row(int_val=4789,
                           string_val="Hello Darkness",
                           date_val=datetime.date(2021, 1, 14)),
                       Row(int_val=4790,
                           string_val="My Old Friend",
                           date_val=datetime.date(2021, 1, 15))],
            double_val=43.102
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (43.102, 4789, "Hello Darkness", datetime.date(2021, 1, 14)),
            (43.102, 4790, "My Old Friend", datetime.date(2021, 1, 15))],
            schema=["double_val", "array_val_exploded_int_val",
                    "array_val_exploded_string_val", "array_val_exploded_date_val"])

        assert_df_equality(expected_output_df, output_df)


class TestDataFrameContainingStructs:
    def test_single_struct_single_attribute(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            struct_val=Row(int_val=4789)
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, )], schema=["struct_val_int_val"])

        assert_df_equality(expected_output_df, output_df)

    def test_single_struct_multiple_attributes(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            struct_val=Row(int_val=4789, string_val="Hello World")
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, "Hello World")], schema=["struct_val_int_val", "struct_val_string_val"])

        assert_df_equality(expected_output_df, output_df)

    def test_heavily_nested_struct_attributes(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            struct_val_1=Row(
                struct_val_2=Row(
                    struct_val_3=Row(
                        struct_val_4=Row(int_val=4789),
                        long_val=478934243342334),
                    string_val="Hello"),
                double_val=43.12),
            timestamp_val=datetime.datetime(2021, 1, 1, 12, 30, 15)
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame(
            [(4789, 478934243342334, "Hello", 43.12, datetime.datetime(2021, 1, 1, 12, 30, 15))],
            schema=["struct_val_1_struct_val_2_struct_val_3_struct_val_4_int_val",
                    "struct_val_1_struct_val_2_struct_val_3_long_val",
                    "struct_val_1_struct_val_2_string_val",
                    "struct_val_1_double_val",
                    "timestamp_val"]
        )
        assert_df_equality(expected_output_df, output_df)


#
# class TestExploding(object):
#     @pytest.fixture(scope="module")
#     def input_df(self, spark_session):
#         return spark_session.read.parquet("data/schema_v1/parquetFiles")
#
#     @pytest.fixture()
#     def default_params(self):
#         return {"path_to_array": "attributes.friends", "exploded_elem_name": "friend"}
#
#     @pytest.mark.slow
#     def test_count(self, input_df, default_params):
#         expected_count = input_df.select(sql_funcs.explode(input_df[default_params["path_to_array"]])).count()
#         actual_count = Exploder(**default_params).transform(input_df).count()
#         assert expected_count == actual_count
#
#     @pytest.mark.slow
#     def test_exploded_array_is_added(self, input_df, default_params):
#         transformer = Exploder(**default_params)
#         expected_columns = set(input_df.columns + [default_params["exploded_elem_name"]])
#         actual_columns = set(transformer.transform(input_df).columns)
#
#         assert expected_columns == actual_columns
#
#     @pytest.mark.slow
#     def test_array_is_converted_to_struct(self, input_df, default_params):
#         def get_data_type_of_column(df, path=["attributes"]):
#             record = df.first().asDict(recursive=True)
#             for p in path:
#                 record = record[p]
#             return type(record)
#
#         current_data_type_friend = get_data_type_of_column(input_df, path=["attributes", "friends"])
#         assert issubclass(current_data_type_friend, list)
#
#         transformed_df = Exploder(**default_params).transform(input_df)
#         transformed_data_type = get_data_type_of_column(transformed_df, path=["friend"])
#
#         assert issubclass(transformed_data_type, dict)
#
#     def test_records_with_empty_arrays_are_dropped_by_default(self, spark_session):
#         input_df = spark_session.createDataFrame([
#             Row(id=1, array_to_explode=[]),
#             Row(id=2, array_to_explode=[Row(elem_id="a"), Row(elem_id="b"), Row(elem_id="c")]),
#             Row(id=3, array_to_explode=[]),
#         ])
#         transformed_df = Exploder(path_to_array="array_to_explode", exploded_elem_name="elem").transform(input_df)
#         assert transformed_df.count() == 3
#
#     def test_records_with_empty_arrays_are_kept_via_setting(self, spark_session):
#         input_df = spark_session.createDataFrame([
#             Row(id=1, array_to_explode=[]),
#             Row(id=2, array_to_explode=[Row(elem_id="a"), Row(elem_id="b"), Row(elem_id="c")]),
#             Row(id=3, array_to_explode=[]),
#         ])
#         transformed_df = Exploder(path_to_array="array_to_explode",
#                                   exploded_elem_name="elem",
#                                   drop_rows_with_empty_array=False).transform(input_df)
#         assert transformed_df.count() == 5
#
