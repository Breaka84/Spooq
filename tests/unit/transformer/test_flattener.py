import pytest
from chispa.dataframe_comparer import assert_df_equality as chispa_assert_df_equality
from pyspark.sql import Row
import datetime
import json

from spooq2.transformer import Flattener
from ...helpers.skip_conditions import only_spark3


@pytest.fixture
def flattener():
    return Flattener(pretty_names=False)


def assert_mapping_equality(mapping_1, mapping_2, spark):
    if mapping_1 == mapping_2:
        return True
    else:  # for easier debugging
        assert_df_equality(
            spark.createDataFrame(mapping_1, ["name", "source", "type"]),
            spark.createDataFrame(mapping_2, ["name", "source", "type"])
        )


def assert_df_equality(df1, df2, ignore_nullable=True, reorder_dataframes=False):
    spark_session = df1.sql_ctx

    def order_dict(dictionary):
        result = {}
        for k, v in sorted(dictionary.items()):
            if isinstance(v, dict):
                result[k] = order_dict(v)
            else:
                result[k] = v
        return result

    if reorder_dataframes:
        df1_json_list = [json.dumps(order_dict(json.loads(json_string))) for json_string in df1.toJSON().collect()]
        df2_json_list = [json.dumps(order_dict(json.loads(json_string))) for json_string in df2.toJSON().collect()]
        df1 = spark_session.read.json(spark_session._sc.parallelize(df1_json_list))
        df2 = spark_session.read.json(spark_session._sc.parallelize(df2_json_list))

    return chispa_assert_df_equality(df1, df2, ignore_nullable)


class TestBasicAttributes:
    """Mapper for Flattening DataFrames"""

    def test_logger_should_be_accessible(self, flattener):
        assert hasattr(flattener, "logger")

    def test_name_is_set(self, flattener):
        assert flattener.name == "Flattener"

    def test_str_representation_is_correct(self, flattener):
        assert str(flattener) == "Transformer Object of Class Flattener"


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

    def test_multiple_columns_of_different_datatype(self, spark_session, spark_major_version, flattener):
        input_df = spark_session.createDataFrame([Row(
            int_val=4789,
            string_val="Hello World",
            date_val=datetime.date(2021, 1, 14)
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, "Hello World", datetime.date(2021, 1, 14))], schema=["int_val", "string_val", "date_val"])

        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)

    def test_multiple_columns_of_different_datatype_keeping_original_columns(self, spark_session, spark_major_version):
        input_df = spark_session.createDataFrame([Row(
            int_val=4789,
            string_val="Hello World",
            date_val=datetime.date(2021, 1, 14)
        )])
        flattener = Flattener(keep_original_columns=True)
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            Row(original_columns=Row(int_val=4789, string_val="Hello World", date_val=datetime.date(2021, 1, 14)),
                int_val=4789, string_val="Hello World", date_val=datetime.date(2021, 1, 14))])
        expected_output_df.schema["original_columns"].nullable = False
        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)


class TestDataFrameContainingArrays:
    def test_single_array(self, spark_session, flattener):
        input_df = spark_session.createDataFrame([Row(
            array_val=[4789, 4790, 4791]
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            (4789, ), (4790, ), (4791, )], schema=["array_val"])

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
            schema=["timestamp_val", "array_val"])

        assert_df_equality(expected_output_df, output_df)

    def test_single_array_with_other_columns_keeping_original_columns(self, spark_session, spark_major_version):
        input_df = spark_session.createDataFrame([Row(
            array_val=[4789, 4790, 4791],
            timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14)
        )])
        flattener = Flattener(keep_original_columns=True)
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame([
            Row(original_columns=Row(array_val=[4789, 4790, 4791], timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14)),
                timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14), array_val=4789),
            Row(original_columns=Row(array_val=[4789, 4790, 4791], timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14)),
                timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14), array_val=4790),
            Row(original_columns=Row(array_val=[4789, 4790, 4791], timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14)),
                timestamp_val=datetime.datetime(2021, 1, 14, 8, 10, 14), array_val=4791),
        ])
        expected_output_df.schema["original_columns"].nullable = False
        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)

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
            schema=["array_val_1", "array_val_2"])

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
            schema=["double_val", "array_val_1", "array_val_2"])

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
            schema=["string_val", "array_val"])

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
            schema=["struct_val_string_val", "struct_val_array_val"])

        assert_df_equality(expected_output_df, output_df)

    def test_struct_nested_in_array(self, spark_session, flattener, spark_major_version):
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
            schema=["double_val", "array_val_int_val",
                    "array_val_string_val", "array_val_date_val"])

        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)


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

    def test_nested_struct_attributes(self, spark_session, flattener, spark_major_version):
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
        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)


class TestComplexRecipes:

    @pytest.fixture(scope="class")
    def input_df(self, spark_session):
        """Taken from https://opensource.adobe.com/Spry/samples/data_region/JSONDataSetSample.html"""
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

    @pytest.fixture(scope="class")
    def expected_output_data(self):
        return [
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
            ("0001", "Cake", 0.55, "donut", "1004", "Devil's Food", "5004", "Maple"                    )]

    @pytest.fixture(scope="class")
    def expected_output_df(self, expected_output_data, spark_session):
        return spark_session.createDataFrame(
            expected_output_data,
            schema=["id", "name", "ppu", "type", "batters_batter_id", "batters_batter_type",
                    "topping_id", "topping_type"])

    @pytest.fixture(scope="class")
    def expected_output_df_pretty(self, expected_output_data, spark_session):
        return spark_session.createDataFrame(
            expected_output_data,
            schema=["id", "name", "ppu", "type", "batter_id", "batter_type",
                    "topping_id", "topping_type"])

    @pytest.fixture(scope="class")
    def expected_output_df_pretty_with_original_columns(self, expected_output_data, input_df, spark_session):
        original_columns = input_df.first()
        expected_output_data_with_original_columns = [
            (original_columns, *row)
            for row
            in expected_output_data
        ]
        output_df = spark_session.createDataFrame(
            expected_output_data_with_original_columns,
            schema=["original_columns", "id", "name", "ppu", "type", "batter_id", "batter_type",
                    "topping_id", "topping_type"])
        output_df.schema["original_columns"].nullable = False
        return output_df

    def test_donut(self, input_df, expected_output_df, flattener):
        output_df = flattener.transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_pretty_donut(self, input_df, expected_output_df_pretty):
        flattener = Flattener(pretty_names=True)
        output_df = flattener.transform(input_df)
        assert_df_equality(expected_output_df_pretty, output_df)

    def test_pretty_donut_with_original_columns(self, input_df, expected_output_df_pretty_with_original_columns):
        flattener = Flattener(pretty_names=True, keep_original_columns=True)
        output_df = flattener.transform(input_df)
        assert_df_equality(expected_output_df_pretty_with_original_columns, output_df)


class TestPrettyColumnNames:

    @pytest.fixture
    def flattener(self):
        return Flattener(pretty_names=True)

    def test_simple_renames(self, flattener, spark_session):
        input_df = spark_session.createDataFrame([Row(
            struct_val=Row(int_val=4789, string_val="Hello World")
        )])
        expected_output_df = spark_session.createDataFrame([
            (4789, "Hello World")], schema=["int_val", "string_val"])
        output_df = flattener.transform(input_df)
        assert_df_equality(output_df, expected_output_df)

    def test_duplicated_column_names(self, flattener, spark_session):
        input_df = spark_session.createDataFrame([Row(
            struct_val=Row(int_val=4789, string_val="Hello World"),
            struct_val_2=Row(int_val=4790, string_val="How are you?")
        )])
        expected_output_df = spark_session.createDataFrame([
            (4789, "Hello World", 4790, "How are you?")], schema=["int_val", "string_val", "struct_val_2_int_val", "struct_val_2_string_val"])
        output_df = flattener.transform(input_df)
        assert_df_equality(output_df, expected_output_df)

    @only_spark3
    def test_nested_struct_attributes(self, flattener, spark_session):
        input_df = spark_session.createDataFrame([Row(
            struct_val_1=Row(
                struct_val_2=Row(
                    struct_val_3=Row(
                        struct_val_4=Row(int_val=4789),
                        int_val=4790),
                    string_val="Hello"),
                double_val=43.12),
            timestamp_val=datetime.datetime(2021, 1, 1, 12, 30, 15)
        )])
        output_df = flattener.transform(input_df)
        expected_output_df = spark_session.createDataFrame(
            [(4789, 4790, "Hello", 43.12, datetime.datetime(2021, 1, 1, 12, 30, 15))],
            schema=["int_val", "struct_val_3_int_val", "string_val", "double_val", "timestamp_val"]
        )
        assert_df_equality(expected_output_df, output_df)

    def test_multiple_arrays_with_other_columns(self, flattener, spark_session):
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
            schema=["double_val", "array_val_1", "array_val_2"])

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
            schema=["string_val", "array_val"])

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
            schema=["string_val", "array_val"])

        assert_df_equality(expected_output_df, output_df)

    def test_struct_nested_in_array(self, spark_session, flattener, spark_major_version):
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
            schema=["double_val", "int_val", "string_val", "date_val"])

        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)


class TestKeepOriginalColumns:

    @pytest.fixture
    def flattener(self):
        return Flattener(keep_original_columns=True)

    def test_simple_struct(self, flattener, spark_session, spark_major_version):
        input_df = spark_session.createDataFrame([Row(
            struct_val=Row(int_val=4789, string_val="Hello World")
        )])
        expected_output_df = spark_session.createDataFrame([Row(
            original_columns=Row(struct_val=Row(int_val=4789, string_val="Hello World")),
            int_val=4789,
            string_val="Hello World"
        )])
        expected_output_df.schema["original_columns"].nullable = False
        output_df = flattener.transform(input_df)
        if spark_major_version == "2":
            reorder_dataframes = True
        else:
            reorder_dataframes = False
        assert_df_equality(expected_output_df, output_df, reorder_dataframes=reorder_dataframes)
