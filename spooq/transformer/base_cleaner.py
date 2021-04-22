"""
This abstract class provides the functionality to log any cleansed values into a separate column
that contains a struct with a sub column per cleansed column (according to the `cleaning_definition`).
If a value was cleansed, the original value will be stored in its respective sub column. If a value was not
cleansed, the sub column will be empty (None).
"""

from pyspark.sql import functions as F

from .transformer import Transformer


class BaseCleaner(Transformer):
    def __init__(self, cleaning_definitions, column_to_log_cleansed_values):
        super().__init__()
        self.cleaning_definitions = cleaning_definitions
        self.column_to_log_cleansed_values = column_to_log_cleansed_values

    def _get_temporary_column_names(self, column_names):
        return [f"{self.TEMPORARY_COLUMNS_PREFIX}_{column_name}" for column_name in column_names]

    def _add_temporary_columns(self, input_df, column_names, temporary_column_names):
        for column_name, temporary_column_name in zip(column_names, temporary_column_names):
            # copy columns to be cleansed to temporary column
            input_df = input_df.withColumn(temporary_column_name, F.col(column_name))
        return input_df

    def _log_cleansed_values(self, input_df, column_names, temporary_column_names):
        def _only_keep_cleansed_values(column_name, temporary_column_name):
            return F.when(F.col(temporary_column_name) == F.col(column_name), F.lit(None)).otherwise(
                F.col(temporary_column_name)
            )

        for column_name, temporary_column_name in zip(column_names, temporary_column_names):
            # Only keep cleansed values in temporary columns
            input_df = input_df.withColumn(
                temporary_column_name, _only_keep_cleansed_values(column_name, temporary_column_name)
            )

        return input_df.withColumn(
            self.column_to_log_cleansed_values,
            F.struct(
                [
                    F.col(temp_col_name).alias(col_name)
                    for col_name, temp_col_name in zip(column_names, temporary_column_names)
                ]
            ),
        ).drop(*temporary_column_names)
