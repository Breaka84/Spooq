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

    def test_nested_struct_attributes(self, spark_session, flattener):
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


class TestComplexRecipes:
    @pytest.fixture()
    def input_df(self, spark_session):
        return spark_session.createDataFrame([Row(
            batters=Row(
                batter=[Row(id="1001", type="Regular"),
                        Row(id="1002", type="Chocolate"),
                        Row(id="1003", type="Blueberry"),
                        Row(id="1004", type="Devil's Food")]),
            id="0001",
            name="Cake",
            ppu=0.55,
            topping=[Row(id="5001", type="None"),
                     Row(id="5002", type="Glazed"),
                     Row(id="5005", type="Sugar"),
                     Row(id="5007", type="Powdered Sugar"),
                     Row(id="5006", type="Chocolate with Sprinkles"),
                     Row(id="5003", type="Chocolate"),
                     Row(id="5004", type="Maple")],
            type="donut",
        )])

    @pytest.fixture()
    def expected_output_df(self, spark_session):
        return spark_session.createDataFrame([
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5001", "None"                     ),
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5002", "Glazed"                   ),
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5005", "Sugar"                    ),
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5007", "Powdered Sugar"           ),
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5006", "Chocolate with Sprinkles" ),
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5003", "Chocolate"                ),
            ("0001", "Cake", 0.55, "donut", "1001", "Regular",      "5004", "Maple"                    ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5001", "None"                     ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5002", "Glazed"                   ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5005", "Sugar"                    ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5007", "Powdered Sugar"           ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5006", "Chocolate with Sprinkles" ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5003", "Chocolate"                ),
            ("0001", "Cake", 0.55, "donut", "1002", "Chocolate",    "5004", "Maple"                    ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5001", "None"                     ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5002", "Glazed"                   ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5005", "Sugar"                    ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5007", "Powdered Sugar"           ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5006", "Chocolate with Sprinkles" ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5003", "Chocolate"                ),
            ("0001", "Cake", 0.55, "donut", "1003", "Blueberry",    "5004", "Maple"                    ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5001", "None"                     ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5002", "Glazed"                   ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5005", "Sugar"                    ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5007", "Powdered Sugar"           ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5006", "Chocolate with Sprinkles" ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5003", "Chocolate"                ),
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5004", "Maple"                    )],
            schema=["id", "name", "ppu", "type", "batters_batter_exploded_id", "batters_batter_exploded_type",
                    "topping_exploded_id", "topping_exploded_type", ]
        )

    def test_donut(self, input_df, expected_output_df):
        output_df = Flattener().transform(input_df)
        assert_df_equality(expected_output_df, output_df)
