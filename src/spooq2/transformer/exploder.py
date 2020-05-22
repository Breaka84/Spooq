from pyspark.sql import functions as f

from transformer import Transformer


class Exploder(Transformer):
    """
    Explodes an array within a DataFrame and
    drops the column containing the source array.

    Examples
    --------
    >>> transformer = Exploder(
    >>>     path_to_array="attributes.friends",
    >>>     exploded_elem_name="friend",
    >>> )

    Parameters
    ----------
    path_to_array : :any:`str`, (Defaults to 'included')
        Defines the Column Name / Path to the Array.
        Dropping nested columns is not supported.
        Although, you can still explode them.

    exploded_elem_name : :any:`str`, (Defaults to 'elem')
        Defines the column name the exploded column will get.
        This is important to know how to access the Field afterwards.
        Writing nested columns is not supported.
        The output column has to be first level.

    Warning
    -------
    **Support for nested column:**

    path_to_array:
        PySpark cannot drop a field within a struct. This means the specific field
        can be referenced and therefore exploded, but not dropped.
    exploded_elem_name:
        If you (re)name a column in the dot notation, is creates a first level column,
        just with a dot its name. To create a struct with the column as a field
        you have to redefine the structure or use a UDF.

    Note
    ----
    The :meth:`~spark.sql.functions.explode` method of Spark is used internally.

    Note
    ----
    The size of the resulting DataFrame is not guaranteed to be
    equal to the Input DataFrame!
    """

    def __init__(self, path_to_array="included", exploded_elem_name="elem"):
        super(Exploder, self).__init__()
        self.path_to_array = path_to_array
        self.exploded_elem_name = exploded_elem_name

    def transform(self, input_df):
        return input_df.withColumn(
            self.exploded_elem_name, f.explode(self.path_to_array)
        ).drop(self.path_to_array)
