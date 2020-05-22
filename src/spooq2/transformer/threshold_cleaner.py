import pyspark.sql.functions as F
import pyspark.sql.types as sql_types

from transformer import Transformer


class ThresholdCleaner(Transformer):
    """
    Sets outiers within a DataFrame to a default value.
    Takes a dictionary with valid value ranges for each column to be cleaned.

    Example
    -------
    >>> transformer = ThresholdCleaner(
    >>>     thresholds={
    >>>         "created_at": {
    >>>             "min": 0, 
    >>>             "max": 1580737513, 
    >>>             "default": None
    >>>         },
    >>>         "size_cm": {
    >>>             "min": 70, 
    >>>             "max": 250, 
    >>>             "default": None
    >>>         },
    >>>     }
    >>> )
    
    Parameters
    ----------
    thresholds : :py:class:`dict`
        Dictionary containing column names and respective valid ranges

    Returns
    -------
    :any:`pyspark.sql.DataFrame`
        The transformed DataFrame

    Raises
    ------
    :any:`exceptions.ValueError`
        Threshold-based cleaning only supports Numeric Types!
        Column of name: {col_name} and type of: {col_type} was provided
        
    Warning
    -------
    Only numeric data types are supported!
    """

    def __init__(self, thresholds={}):
        super(ThresholdCleaner, self).__init__()
        self.thresholds = thresholds
        self.logger.debug("Range Definitions: " + str(self.thresholds))

    def transform(self, input_df):
        self.logger.debug("input_df Schema: " + input_df._jdf.schema().treeString())

        ordered_column_names = input_df.columns
        for column_name, value_range in self.thresholds.items():
            
            data_type = input_df.schema[str(column_name)].dataType
            if not isinstance(data_type, sql_types.NumericType):
                raise ValueError(
                    "Threshold-based cleaning only supports Numeric Types!\n",
                    "Column of name: {col_name} and type of: {col_type} was provided".format(
                        col_name=column_name, col_type=data_type
                    ),
                )

            self.logger.debug(
                "Ranges for column " + column_name + ": " + str(value_range)
            )

            input_df = input_df.withColumn(
                column_name,
                F.when(
                    input_df[column_name].between(
                        value_range["min"], value_range["max"]
                    ),
                    input_df[column_name],
                )
                .otherwise(F.lit(value_range.get("default", None)))
                .cast(data_type),
            )

        return input_df.select(ordered_column_names)
