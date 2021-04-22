from __future__ import absolute_import
from builtins import str
from pyspark.sql.window import Window  # noqa: F401
from pyspark.sql.functions import row_number, when

from .transformer import Transformer


class NewestByGroup(Transformer):
    """
    Groups, orders and selects first element per group.

    Example
    -------
    >>> transformer = NewestByGroup(
    >>>     group_by=["first_name", "last_name"],
    >>>     order_by=["created_at_ms", "version"]
    >>> )

    Parameters
    ----------
    group_by : :any:`str` or :any:`list` of :any:`str`, (Defaults to ['id'])
        List of attributes to be used within the Window Function as Grouping Arguments.

    order_by : :any:`str` or :any:`list` of :any:`str`, (Defaults to ['updated_at', 'deleted_at'])
        List of attributes to be used within the Window Function as Ordering Arguments.
        All columns will be sorted in **descending** order.

    Raises
    ------
    :any:`exceptions.AttributeError`
        If any Attribute in :py:data:`group_by` or :py:data:`order_by` is not contained in the
        input DataFrame.

    Note
    ----
    PySpark's :py:class:`~pyspark.sql.Window` function is used internally
    The first row (:py:meth:`~pyspark.sql.functions.row_number`) per window will be selected and returned.
    """

    def __init__(self, group_by=["id"], order_by=["updated_at", "deleted_at"]):
        super(NewestByGroup, self).__init__()
        self.group_by = []
        if isinstance(group_by, list):
            self.group_by.extend(group_by)
        else:
            self.group_by.extend([group_by])

        self.order_by = []
        if isinstance(order_by, list):
            self.order_by.extend(order_by)
        else:
            self.order_by.extend([order_by])

        self.logger.debug("group by columns: " + str(self.group_by))
        self.logger.debug("order by columns: " + str(self.order_by))

    def transform(self, input_df):
        self.logger.debug(
            """{grp_by} used for grouping, {ord_by} used for ordering""".format(
                grp_by=self.group_by, ord_by=self.order_by
            )
        )

        window = self._construct_window_function(input_df, self.group_by, self.order_by)

        return input_df.select("*", row_number().over(window).alias("row_nr")).where("row_nr = 1").drop("row_nr")

    def _construct_window_function(self, input_df, group_by, order_by):
        """Constructs a window function based on the given input params"""
        group_by_query = [input_df[col] for col in group_by]
        order_by_query = [input_df[col].desc_nulls_last() for col in order_by]

        return Window.partitionBy(group_by_query).orderBy(order_by_query)
