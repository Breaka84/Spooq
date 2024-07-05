from typing import List, Dict
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.utils import AnalysisException
from chispa.dataframe_comparer import assert_df_equality

from tests import DATA_FOLDER
from spooq.transformer import Annotator
from spooq.transformer.annotator import update_comment, ColumnNotFound, CommentNotFound, AnnotatorMode


@pytest.fixture(scope="module")
def input_data():
    return [
        Row(col_a=1, col_b=2),
        Row(col_a=4, col_b=5),
    ]


@pytest.fixture(scope="module")
def input_df_without_comments(input_data: List[Row], spark_session: SparkSession):
    return spark_session.createDataFrame(input_data, schema="col_a int, col_b int")


@pytest.fixture(scope="module")
def input_df_with_partial_comments(input_data: List[Row], spark_session: SparkSession):
    return spark_session.createDataFrame(input_data, schema="col_a int COMMENT 'initial', col_b int")


@pytest.fixture()
def table_without_comments(spark_session: SparkSession, input_df_without_comments: DataFrame):
    input_df_without_comments.write.saveAsTable(name="table_without_comments", format="delta", mode="overwrite")
    yield "table_without_comments"
    spark_session.sql("DROP TABLE table_without_comments")


@pytest.fixture()
def table_with_partial_comments(spark_session: SparkSession, input_df_with_partial_comments: DataFrame):
    input_df_with_partial_comments.write.saveAsTable(name="table_with_partial_comments", format="delta", mode="overwrite")
    yield "table_with_partial_comments"
    spark_session.sql("DROP TABLE table_with_partial_comments")


# @pytest.fixture(scope="module")
# def input_df_with_comments(input_data: List[Row], spark_session: SparkSession):
#     return spark_session.createDataFrame(
#         input_data,
#         schema="""
#               col_a int COMMENT 'initial'
#             , col_b int COMMENT 'initial'
#         """
#     )


class TestUpdateCommentsMethod:

    @pytest.fixture(scope="class")
    def input_df(self, input_df_with_partial_comments) -> DataFrame:
        return input_df_with_partial_comments

    def test_add_non_existing_comment(self, input_df: DataFrame):
        output_df = update_comment(df=input_df, column_name="col_b", comment="added", annotator_mode=AnnotatorMode.insert)
        assert output_df.schema["col_b"].metadata.get("comment") == "added"

    def test_add_existing_comment_does_not_overwrite(self, input_df: DataFrame):
        output_df = update_comment(df=input_df, column_name="col_a", comment="added", annotator_mode=AnnotatorMode.insert)
        assert output_df.schema["col_a"].metadata.get("comment") == "initial"

    @pytest.mark.parametrize("comment", ["", "     ", None])
    def test_add_empty_comment(self, comment: str, input_df: DataFrame):
        output_df = update_comment(df=input_df, column_name="col_b", comment=comment, annotator_mode=AnnotatorMode.insert)
        assert output_df.schema["col_b"].metadata.get("comment") == comment

    def test_add_comment_for_missing_column_raises_exception(self, input_df: DataFrame):
        with pytest.raises(ColumnNotFound):
            update_comment(df=input_df, column_name="col_c", comment="added", annotator_mode=AnnotatorMode.insert)

    def test_replace_non_existing_comment(self, input_df: DataFrame):
        output_df = update_comment(df=input_df, column_name="col_b", comment="replaced", annotator_mode=AnnotatorMode.upsert)
        assert output_df.schema["col_b"].metadata.get("comment") == "replaced"

    def test_replace_existing_comment(self, input_df: DataFrame):
        output_df = update_comment(df=input_df, column_name="col_a", comment="replaced", annotator_mode=AnnotatorMode.upsert)
        assert output_df.schema["col_a"].metadata.get("comment") == "replaced"

    @pytest.mark.parametrize("comment", ["", "     ", None])
    def test_replace_with_empty_comment(self, comment: str, input_df: DataFrame):
        output_df = update_comment(df=input_df, column_name="col_b", comment=comment, annotator_mode=AnnotatorMode.upsert)
        assert output_df.schema["col_b"].metadata.get("comment") == comment


class TestAnnotatorTransformer:
    @pytest.fixture(scope="class")
    def comments_mapping(self, input_df_without_comments: DataFrame) -> List[Dict]:
        return {
            col_name: "updated"
            for col_name in input_df_without_comments.columns
        }

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "updated"),
            ("col_b", "updated"),
        ]
    )
    def test_adding_comments_to_non_commmented_table(
        self,
        column_name: str,
        expected_comment: str,
        spark_session: SparkSession,
        comments_mapping: List[Dict],
        input_df_without_comments: DataFrame,
        table_without_comments: str
    ):
        Annotator(comments_mapping).transform(input_df_without_comments).write.saveAsTable(table_without_comments, mode="overwrite")
        table_description_df = spark_session.sql(f"DESCRIBE {table_without_comments}")
        assert table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0] == expected_comment

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "updated"),
            ("col_b", "updated"),
        ]
    )
    def test_add_or_replace_comments_to_partially_commmented_table(
        self,
        column_name: str,
        expected_comment: str,
        spark_session: SparkSession,
        comments_mapping: List[Dict],
        input_df_without_comments: DataFrame,
        table_with_partial_comments: str
    ):
        Annotator(comments_mapping, mode=AnnotatorMode.upsert, sql_source_table_identifier=table_with_partial_comments).transform(input_df_without_comments).write.saveAsTable(table_with_partial_comments, mode="overwrite")
        table_description_df = spark_session.sql(f"DESCRIBE {table_with_partial_comments}")
        assert table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0] == expected_comment

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "initial"),
            ("col_b", "updated"),
        ]
    )
    def test_add_comments_to_partially_commmented_table(
        self,
        column_name: str,
        expected_comment: str,
        spark_session: SparkSession,
        comments_mapping: List[Dict],
        input_df_without_comments: DataFrame,
        table_with_partial_comments: str
    ):
        Annotator(comments_mapping, mode=AnnotatorMode.insert, sql_source_table_identifier=table_with_partial_comments).transform(input_df_without_comments).write.saveAsTable(table_with_partial_comments, mode="overwrite")
        table_description_df = spark_session.sql(f"DESCRIBE {table_with_partial_comments}")
        assert table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0] == expected_comment

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "initial"),
            ("col_b", None),
        ]
    )
    def test_only_fetch_comments_from_partially_commmented_table(
        self,
        column_name: str,
        expected_comment: str,
        spark_session: SparkSession,
        input_df_without_comments: DataFrame,
        table_with_partial_comments: str
    ):
        Annotator(comments_mapping=None, sql_source_table_identifier=table_with_partial_comments).transform(input_df_without_comments).write.saveAsTable(table_with_partial_comments, mode="overwrite")
        table_description_df = spark_session.sql(f"DESCRIBE {table_with_partial_comments}")
        assert table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0] == expected_comment
