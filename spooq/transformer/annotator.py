from enum import Enum
from pathlib import Path
from typing import Union

from pyspark.sql import functions as F, types as T, DataFrame, SparkSession

from spooq.transformer.transformer import Transformer


AnnotatorMode = Enum("mode", ["insert", "upsert"])
MissingColumnHandling = Enum("missing_column_handling", ["raise_error", "skip"])


class ColumnNotFound(ValueError):
    pass


class CommentNotFound(ValueError):
    pass


def load_comments_from_metastore_table(sql_source_table_identifier: str) -> dict:
    spark = SparkSession.Builder().getOrCreate()
    table_description = spark.sql(f"DESCRIBE {sql_source_table_identifier}")
    return {k: v for k, v in table_description.rdd.map(lambda row: (row.col_name, row.comment)).collect() if v}


def update_comment(
    df: DataFrame,
    column_name: str,
    comment: str,
    annotator_mode: AnnotatorMode = AnnotatorMode.upsert,
    missing_column_handling: MissingColumnHandling = MissingColumnHandling.raise_error,
) -> DataFrame:
    if column_name not in df.columns:
        if missing_column_handling == MissingColumnHandling.skip:
            return df
        else:
            raise ColumnNotFound(
                f"Comment <{comment}> cannot be applied to Column <{column_name}> "
                "because the column was not found in the dataframe:\n"
                "\n".join(df.columns)
            )

    existing_comment = df.schema[column_name].metadata.get("comment")
    if annotator_mode == AnnotatorMode.insert and existing_comment:
        return df

    return df.withColumn(column_name, F.col(column_name).alias(column_name, metadata={"comment": comment}))


class Annotator(Transformer):
    def __init__(
        self,
        comments_mapping: Union[str, Path, dict] = None,
        mode: AnnotatorMode = AnnotatorMode.upsert,
        missing_column_handling: MissingColumnHandling = MissingColumnHandling.raise_error,
        sql_source_table_identifier: str = None,
    ):
        super().__init__()
        self.comments_mapping = comments_mapping or {}
        self.mode = mode
        self.missing_column_handling = missing_column_handling
        self.sql_source_table_identifier = sql_source_table_identifier
        """
        # Todo: Put into Docstring
        if mode not in ["insert", "replace"]:
            raise ValueError(
                "Only the following values are allowed for `mode`:\n"
                "  - replace: new comments are added; existing comments are replaced\n"
                "  - insert: new comments are added; existing comments are NOT overwritten"
            )
        """

    def transform(self, input_df: DataFrame) -> DataFrame:
        if self.sql_source_table_identifier:
            self.logger.info(
                f"sql_source_table_identifier was provided: Trying to load existing comments from source table at <{self.sql_source_table_identifier}> ..."
            )
            existing_comments_mapping = load_comments_from_metastore_table(
                sql_source_table_identifier=self.sql_source_table_identifier
            )
            self.logger.info(
                f"Trying to apply {len(existing_comments_mapping)} existing comments from {self.sql_source_table_identifier} to dataframe!"
            )
            input_df = Annotator(
                comments_mapping=existing_comments_mapping,
                mode=AnnotatorMode.insert,
                missing_column_handling=MissingColumnHandling.skip,
                sql_source_table_identifier=None,
            ).transform(input_df)
            self.logger.info(
                f"Re-applied {len(set(input_df.columns).intersection(set(existing_comments_mapping)))} comments from {self.sql_source_table_identifier}!"
            )

        self.logger.info(f"Applying {len(self.comments_mapping)} comments from mapping to DataFrame ...")
        for column, comment in self.comments_mapping.items():
            input_df = update_comment(
                df=input_df,
                column_name=column,
                comment=comment,
                annotator_mode=self.mode,
                missing_column_handling=self.missing_column_handling,
            )
        return input_df
