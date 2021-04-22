from __future__ import absolute_import
from past.builtins import basestring
from .transformer import Transformer


class Sieve(Transformer):
    """
    Filters rows depending on provided filter expression.
    Only records complying with filter condition are kept.

    Examples
    --------
    >>> transformer = T.Sieve(filter_expression=\"\"\" attributes.last_name rlike "^.{7}$" \"\"\")

    >>> transformer = T.Sieve(filter_expression=\"\"\" lower(gender) = "f" \"\"\")

    Parameters
    ----------
    filter_expression  : :any:`str`
        A valid PySpark SQL expression which returns a boolean

    Raises
    ------
    :any:`exceptions.ValueError`
         filter_expression has to be a valid (Spark)SQL expression provided as a string

    Note
    ----
    The :meth:`~pyspark.sql.DataFrame.filter` method is used internally.

    Note
    ----
    The Size of the resulting DataFrame is not guaranteed to be equal to the Input DataFrame!
    """

    def __init__(self, filter_expression):
        super(Sieve, self).__init__()
        self.filter_expression = filter_expression

        if not isinstance(filter_expression, basestring):
            raise ValueError(
                "filter_expression has to be a valid (Spark)SQL expression ",
                "provided as a string",
            )

    def transform(self, input_df):
        return input_df.filter(self.filter_expression)
