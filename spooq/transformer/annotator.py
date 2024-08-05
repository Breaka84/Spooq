import logging
from pathlib import Path
from typing import Dict, Union

from pyspark.sql import functions as F, types as T, DataFrame, SparkSession

from spooq.shared import EnumMode
from spooq.transformer.transformer import Transformer


class AnnotatorMode(EnumMode):
    """Possible values: ['insert', 'upsert']"""

    insert = "Only add missing comments. Don't overwrite existing comments"
    upsert = "Insert missing comments. Overwrite existing comments"


class MissingColumnHandling(EnumMode):
    """Possible values: ['raise_error', 'skip']"""

    raise_error = "Raise an error when a comment was defined for a missing column"
    skip = "Skip / ignore each comment that refers to a missing column"


class ColumnNotFound(ValueError):
    pass


def load_comments_from_metastore_table(sql_source_table_identifier: str) -> Dict[str, str]:
    """
    Extracts comments from metadata stored in table (loaded by path or metastore).

    Parameters
    ----------
    sql_source_table_identifier : :any:`str`
        This is the fully qualified table name used for the ``DESCRIBE <sql_source_table_identifier>`` SQL command.
        Some examples:

            * delta.`/path/to/table/files.delta`
            * my_db.my_table
            * my_catalog.my_db.my_table

    Returns
    -------
    :any:`dict`
        Dictionary containing all extracted, non-null comments per column. ({<column_name>: <comment>})
    """
    spark = SparkSession.Builder().getOrCreate()
    table_description = spark.sql(f"DESCRIBE {sql_source_table_identifier}")
    return {k: v for k, v in table_description.rdd.map(lambda row: (row.col_name, row.comment)).collect() if v}


def update_comment(
    df: DataFrame,
    column_name: str,
    comment: str,
    annotator_mode: AnnotatorMode = AnnotatorMode.upsert,
    missing_column_handling: MissingColumnHandling = MissingColumnHandling.raise_error,
    logger: logging.Logger = None,
) -> DataFrame:
    """
    Updates a single column's comment within the provided dataframe.

    Parameters
    ----------
        df : |SPARK_DATAFRAME|
            The dataframe that contains the column to be updated.
        column_name : :any:`str`
            The name of the column to be commented.
        comment : :any:`str`
            The comment to apply to the referenced column.
        annotator_mode : :py:class:`AnnotatorMode`, defaults to AnnotatorMode.upsert
            Defines if existing columns get overwritten.
        missing_column_handling : :py:class:`MissingColumnHandling`, defaults to ``MissingColumnHandling.raise_error``
            Defines how to behave when the ``column_name`` is not found in the dataframe.

    Raises:
        ColumnNotFound: If column is missing from the dataframe and missing_column_handling is set to ``raise_error``

    Returns:
        |SPARK_DATAFRAME|: Dataframe with updated metadata (comment) for the specified column.
    """
    logger = logger or logging.getLogger("spooq")
    if column_name not in df.columns:
        if missing_column_handling == MissingColumnHandling.skip:
            logger.warning(
                f"Column '{column_name}' was not found in the dataframe. MissingColumnHandling is set to 'skip'"
            )
            return df
        else:
            raise ColumnNotFound(
                f"Comment <{comment}> cannot be applied to Column <{column_name}> "
                "because the column was not found in the dataframe:\n"
                "\n".join(df.columns)
            )

    existing_comment = df.schema[column_name].metadata.get("comment")
    if annotator_mode == AnnotatorMode.insert and existing_comment:
        logger.info(
            f"A comment already exists for column '{column_name}' but the annotation mode is set to insert only "
            f"-> skipping the update. Existing comment: '{existing_comment}', New comment: '{comment}'"
        )
        return df

    return df.withColumn(column_name, F.col(column_name).alias(column_name, metadata={"comment": comment}))


class Annotator(Transformer):
    """
    Inserts or upserts column comments to dataframes. Can also be used just to fetch column comments
    from an existing table.

    Parameters
    ----------
    comments_mapping  : :class:`dict`, Defaults to {}
        Dictionary consisting of column names and comments
    mode : :py:class:`AnnotatorMode`, Defaults to ``AnnotatorMode.upsert``
        This mode defines how the transformer should react to existing column comments. ``insert`` will leave existing
        untouched while ``upsert`` overwrites them if a new comment is provided. Existing columns are defined as
        comments already defined in the dataframe or the sql_source_table!
    missing_column_handling : :py:class:`MissingColumnHandling`, Defaults to ``MissingColumnHandling.raise_error``
        This mode defines how the transformer should react to missing columns that are referenced in
        the ``comments_mapping``.
    sql_source_table_identifier : :any:`str`, Defaults to None
        The transformer will load any existing comments from the defined source table to the ``comments_mapping``

    Example
    -------
    >>> '''Fetching comments from an existing source table and applying those plus explicitely defined comments'''
    >>> # Schema of sql_source_table (/tmp/path/to/my/silver_table.delta):
    >>> #     col_A string COMMENT "Comment from sql_source_table",
    >>> #     col_B string COMMENT "Comment from sql_source_table",
    >>> #     col_Z string COMMENT "Comment from sql_source_table",
    >>>
    >>> # Schema of input dataframe
    >>> #     col_A string,
    >>> #     col_B string,
    >>> #     col_Y string,
    >>>
    >>> from spooq.transformer import Annotator
    >>> from spooq.transformer.annotator import AnnotatorMode, MissingColumnHandling
    >>>
    >>> spark.createDataFrame(
    >>>     [],
    >>>     schema='''
    >>>         col_A string COMMENT "Comment from sql_source_table",
    >>>         col_B string COMMENT "Comment from sql_source_table",
    >>>         col_Z string COMMENT "Comment from sql_source_table"
    >>>     '''
    >>> ).write.format("delta").mode("overwrite").options(mergeSchema=True).save("/tmp/path/to/my/silver_table.delta")
    >>> input_df = spark.createDataFrame([], "col_A string, col_B string, col_Y string")
    >>> comments_mapping = {
    >>>     "col_A": "Updated comment from comments_mapping",
    >>>     "col_Y": "New comment from comments_mapping",
    >>> }
    >>>
    >>> output_df = Annotator(
    >>>     comments_mapping=comments_mapping,
    >>>     mode=AnnotatorMode.upsert,
    >>>     missing_column_handling=MissingColumnHandling.raise_error,
    >>>     sql_source_table_identifier="delta.`/tmp/path/to/my/silver_table.delta`",
    >>> ).transform(input_df)
    >>>
    >>> print(json.dumps({col["name"]: col["metadata"]["comment"] for col in output_df.schema.J["fields"]}, indent=2))
    {
    "col_A": "Updated comment from comments_mapping",
    "col_B": "Comment from sql_source_table",
    "col_Y": "New comment from comments_mapping"
    }

    Note
    ----
    Here are some use cases to use this transformer:
        - Add explicitely defined comments from ``comments_mapping`` to dataframe within silver pipeline
        - Apply comments from existing silver table to dataframe within gold pipeline
        - Apply comments from ``columns_mapping`` within the :py:class:`~spooq.transformer.mapper.Mapper` transformer
        - Apply comments from existing silver table to gold dataframe
          within the :py:class:`~spooq.transformer.mapper.Mapper` transformer
        - Combine any of them

    Raises
    ------
        ColumnNotFound: If column is missing from the dataframe and missing_column_handling is set to ``raise_error``
    """

    def __init__(
        self,
        comments_mapping: Dict[str, str] = None,
        mode: AnnotatorMode = AnnotatorMode.upsert,
        missing_column_handling: MissingColumnHandling = MissingColumnHandling.raise_error,
        sql_source_table_identifier: str = None,
    ):
        super().__init__()
        self.comments_mapping = comments_mapping or {}
        self.mode = mode
        self.missing_column_handling = missing_column_handling
        self.sql_source_table_identifier = sql_source_table_identifier

    def transform(self, input_df: DataFrame) -> DataFrame:
        if self.sql_source_table_identifier:
            self.logger.info(
                f"sql_source_table_identifier was provided: "
                f"Trying to load existing comments from source table at <{self.sql_source_table_identifier}> ..."
            )
            existing_comments_mapping = load_comments_from_metastore_table(
                sql_source_table_identifier=self.sql_source_table_identifier
            )
            self.logger.info(
                f"Trying to add {len(existing_comments_mapping)} existing comments "
                f"from {self.sql_source_table_identifier} to provided comments_mapping!"
            )
            for column, existing_comment in existing_comments_mapping.items():
                if not column in input_df.columns:
                    self.logger.debug(
                        f"Skipping to apply comment ('{existing_comment}') fetched from the sql_source_table because "
                        f"column: '{column}' was not found in the dataframe."
                    )
                    continue

                if column in self.comments_mapping:
                    self.logger.debug(
                        f"Comment for column: '{column}' is both defined in the sql_source_table ('{existing_comment}') "
                        f"and the mapping ('{self.comments_mapping[column]}')."
                    )
                    if self.mode == AnnotatorMode.upsert:
                        self.logger.debug("Using comment from mapping.")
                        continue

                self.logger.debug("Using comment from sql_source_table.")
                self.comments_mapping[column] = existing_comment

            self.logger.info(
                f"Added {len(set(input_df.columns).intersection(set(existing_comments_mapping)))} "
                f"comments from {self.sql_source_table_identifier}!"
            )

        self.logger.info(f"Applying {len(self.comments_mapping)} comments from mapping to DataFrame ...")
        for column, comment in self.comments_mapping.items():
            input_df = update_comment(
                df=input_df,
                column_name=column,
                comment=comment,
                annotator_mode=self.mode,
                missing_column_handling=self.missing_column_handling,
                logger=self.logger,
            )
        return input_df
